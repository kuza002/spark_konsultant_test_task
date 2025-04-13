package services

import models._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import scala.util.{Failure, Success, Try}

object EventParser {
  private val formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")

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

      val linesList = iter.toList
      val linesIterator = linesList.iterator

      val results = collection.mutable.ArrayBuffer.empty[(Option[Event], Option[String])]

      while (linesIterator.hasNext) {
        val line = linesIterator.next()
        val result = parseSingleLine(line, linesIterator)
        results += result

      }

      val events = results.flatMap(_._1)
      val errors = results.flatMap(_._2)
      Iterator((events, errors))
    }

    (parsed.flatMap(_._1), parsed.flatMap(_._2))
  }

  private def parsePartition(iter: Iterator[String]): Iterator[Event] = {
    val linesList = iter.toList
    val linesIterator = linesList.iterator

    val events = collection.mutable.ArrayBuffer.empty[Event]

    while (linesIterator.hasNext) {
      val line = linesIterator.next()
      parseSingleLine(line, linesIterator) match {
        case (Some(event), _) => events += event
        case _ =>
      }
    }

    events.iterator
  }

  private def isTimeStamp(string: String): Boolean = {
    Try(java.time.LocalDateTime.parse(string, formatter)).isSuccess
  }

  private def parseTimestamp(timeStr: String): Timestamp = {
    val localDateTime = java.time.LocalDateTime.parse(timeStr, formatter)
    Timestamp.valueOf(localDateTime)
  }

  private def parseSingleLine(line: String, linesIter: Iterator[String]): (Option[Event], Option[String]) = {
    if (line == null || line.trim.isEmpty) return (None, None)

    val parts = line.split(" ", 3)
    if (parts.length < 2) {
      return (None, Some(s"Invalid line format: $line"))
    }

    Try {
      val timeStr = parts(1)
      if (!isTimeStamp(timeStr)) {
        return (None, None)
      }

      val timestamp = parseTimestamp(timeStr)
      parts(0) match {
        case "SESSION_START" =>
          (Some(SessionStart(timestamp)), None)

        case "SESSION_END" =>
          (Some(SessionEnd(timestamp)), None)

        case "QS" if parts.length >= 3 =>
          val query = parts(2).stripPrefix("{").stripSuffix("}")
          if (linesIter.hasNext) {
            val nextLine = linesIter.next()
            val nextParts = nextLine.split("\\s+")
            if (nextParts.nonEmpty && nextParts.head.matches("-?\\d+")) {
              val searchId = nextParts.head.toLong
              val documents = nextParts.tail.toList
              (Some(QuickSearch(timestamp, query, searchId, documents)), None)
            } else {
              (None, Some(s"Invalid search results for QS: $line"))
            }
          } else {
            (None, Some(s"Missing next line for QS results: $line"))
          }

        case "DOC_OPEN" if parts.length >= 3 =>
          val docParts = parts(2).split("\\s+", 2)
          if (docParts.length == 2 && docParts(0).matches("-?\\d+")) {
            val searchId = docParts(0).toLong
            val documentId = docParts(1)
            (Some(DocumentOpen(timestamp, searchId, documentId)), None)
          } else {
            (None, Some(s"Invalid DOC_OPEN format: $line"))
          }

        case "CARD_SEARCH_START" =>
          // Собираем все строки до CARD_SEARCH_END
          val cardSearchLines = new collection.mutable.ArrayBuffer[String]()
          var foundEnd = false
          var searchResults: Option[String] = None

          while (linesIter.hasNext && !foundEnd) {
            val nextLine = linesIter.next()
            if (nextLine.trim == "CARD_SEARCH_END") {
              foundEnd = true
              // Берем следующую строку после END как результаты
              if (linesIter.hasNext) {
                searchResults = Some(linesIter.next())
              }
            } else {
              cardSearchLines += nextLine
            }
          }

          if (!foundEnd) {
            return (None, Some(s"Missing CARD_SEARCH_END for: $line"))
          }

          // Парсим параметры
          val params = cardSearchLines
            .filter(_.startsWith("$"))
            .flatMap { param =>
              param.split("\\s+", 2) match {
                case Array(id, value) =>
                  Try(id.stripPrefix("$").toInt).toOption.map(_ -> value)
                case _ => None
              }
            }.toMap

          // Парсим результаты поиска
          val (searchId, documents) = searchResults match {
            case Some(results) =>
              val resultParts = results.split("\\s+")
              if (resultParts.nonEmpty && resultParts.head.matches("-?\\d+")) {
                (resultParts.head.toLong, resultParts.tail.toList)
              } else {
                (0L, List.empty[String])
              }
            case None =>
              (0L, List.empty[String])
          }

          (Some(CardSearch(timestamp, params, searchId, documents)), None)

        case eventType =>
          (None, Some(s"Unknown event type: $eventType"))
      }
    }.recover {
      case e: Exception =>
        (None, Some(s"Failed to parse line '$line': ${e.getMessage}"))
    }.get
  }

  def convertToDataFrame(spark: SparkSession, events: RDD[Event]): DataFrame = {
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