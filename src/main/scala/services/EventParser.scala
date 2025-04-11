package services

import models._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import scala.util.Try

object EventParser {
  private val formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")

  // Определяем схему DataFrame
  private val schema = StructType(Array(
    StructField("eventType", StringType, nullable = false),
    StructField("timestamp", TimestampType, nullable = false),
    StructField("query", StringType, nullable = true),
    StructField("searchId", LongType, nullable = true),
    StructField("documents", ArrayType(StringType), nullable = true),
    StructField("parameters", MapType(IntegerType, StringType), nullable = true),
    StructField("documentId", StringType, nullable = true)
  ))

  def parse(lines: RDD[String]): RDD[Event] = {
    lines.mapPartitions(parsePartition)
  }

  def parseWithErrors(lines: RDD[String]): (RDD[Event], RDD[String]) = {
    val parsed = lines.mapPartitions { iter =>
      val results = iter.map(line => parseSingleLine(line, None)).toList
      val events = results.flatMap(_._1)
      val errors = results.flatMap(_._2)
      Iterator((events, errors))
    }
    (parsed.flatMap(_._1), parsed.flatMap(_._2))
  }

  private def parsePartition(iter: Iterator[String]): Iterator[Event] = {
    val lines = iter.toList
    lines.sliding(2).flatMap {
      case line :: next :: Nil => parseSingleLine(line, Some(next))._1
      case line :: Nil        => parseSingleLine(line, None)._1
      case _                  => None
    }
  }

  private def parseTimestamp(timeStr: String): Timestamp = {
    val localDateTime = java.time.LocalDateTime.parse(timeStr, formatter)
    Timestamp.valueOf(localDateTime)
  }

  private def parseSingleLine(line: String, nextLine: Option[String]): (Option[Event], Option[String]) = {
    if (line == null || line.trim.isEmpty) return (None, None)

    val parts = line.split(" ", 3)
    if (parts.length < 2) {
      return (None, Some(s"Invalid line format: $line"))
    }

    Try {
      val timestamp = parseTimestamp(parts(1))
      parts(0) match {
        case "SESSION_START" => (Some(SessionStart(timestamp)), None)
        case "SESSION_END"   => (Some(SessionEnd(timestamp)), None)

        case "QS" if parts.length >= 3 =>
          val query = parts(2).stripPrefix("{").stripSuffix("}")
          nextLine match {
            case Some(next) =>
              val nextParts = next.split("\\s+")
              if (nextParts.nonEmpty) {
                val searchId = nextParts.head.toLong
                val documents = nextParts.tail.toList
                (Some(QuickSearch(timestamp, query, searchId, documents)), None)
              } else {
                (None, Some(s"Missing search results for QS: $line"))
              }
            case None =>
              (None, Some(s"Missing next line for QS results: $line"))
          }

        case "DOC_OPEN" if parts.length >= 3 =>
          val docParts = parts(2).split("\\s+", 2)
          if (docParts.length == 2) {
            val searchId = docParts(0).toLong
            val documentId = docParts(1)
            (Some(DocumentOpen(timestamp, searchId, documentId)), None)
          } else {
            (None, Some(s"Invalid DOC_OPEN format: $line"))
          }

        case "CARD_SEARCH_START" =>
          val params = nextLine.map { nl =>
            nl.split("\n")
              .takeWhile(_ != "CARD_SEARCH_END")
              .filter(_.startsWith("$"))
              .map { param =>
                val paramParts = param.split("\\s+", 2)
                val paramId = paramParts(0).stripPrefix("$").toInt
                val paramValue = if (paramParts.length > 1) paramParts(1) else ""
                (paramId, paramValue)
              }.toMap
          }.getOrElse(Map.empty)

          val (searchId, docs) = nextLine.flatMap { nl =>
            nl.split("\n")
              .dropWhile(_ != "CARD_SEARCH_END")
              .drop(1)
              .headOption
              .map { resultsLine =>
                val resultParts = resultsLine.split("\\s+")
                (resultParts.head.toLong, resultParts.tail.toList)
              }
          }.getOrElse((0L, List.empty))

          (Some(CardSearch(timestamp, params, searchId, docs)), None)

        case eventType =>
          (None, Some(s"Unknown event type: $eventType"))
      }
    }.recover {
      case e: Exception =>
        (None, Some(s"Failed to parse line '$line': ${e.getMessage}"))
    }.get
  }

  def convertToDataFrame(spark: SparkSession, events: RDD[Event]): DataFrame = {
    import spark.implicits._

    // Преобразуем события в Row с явным указанием типов
    val rows = events.map {
      case e: SessionStart =>
        Row(e.eventType, e.timestamp, null, null, null, null, null)
      case e: SessionEnd =>
        Row(e.eventType, e.timestamp, null, null, null, null, null)
      case e: QuickSearch =>
        Row(e.eventType, e.timestamp, e.query, e.searchId, e.documents.toArray, null, null)
      case e: CardSearch =>
        Row(e.eventType, e.timestamp, null, e.searchId, e.documents.toArray, e.parameters, null)
      case e: DocumentOpen =>
        Row(e.eventType, e.timestamp, null, e.searchId, null, null, e.documentId)
    }

    spark.createDataFrame(rows, schema)
  }
}