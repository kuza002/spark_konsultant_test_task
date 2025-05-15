package com.test.task.service.parsers

import com.test.task.config.AnalyzerConfig
import com.test.task.models._
import com.test.task.service.LogsAccumulator
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.util.Using
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import scala.collection.mutable.ListBuffer


object SessionBuilder {
  val config: AnalyzerConfig = ConfigSource.default.loadOrThrow[AnalyzerConfig]

  def isSearchResultId(line: String): Boolean = {
    if (line.isEmpty) return false

    var idx = 0
    if (line(idx) == '-') idx += 1

    while (idx < line.length) {
      if (!Character.isDigit(line.charAt(idx))) return false
      idx += 1
    }
    true
  }

  def isDocId(input: String): Boolean = {
    val underscoreIndex = input.indexOf('_')

    if (underscoreIndex < 0 || underscoreIndex >= input.length - 1) return false

    val leftPart = input.substring(0, underscoreIndex)
    val rightPart = input.substring(underscoreIndex + 1)

    if (leftPart.isEmpty || rightPart.isEmpty) return false

    def isValidChar(ch: Char): Boolean = Character.isLetterOrDigit(ch) || ch == '_'

    leftPart.forall(isValidChar) && rightPart.forall(isValidChar)
  }

  def parseDocOpenings(line: (String, Int),
                       iter: Iterator[(String, Int)]): Option[((String, Int), Seq[DocumentOpen])] = {

    var nextLine = line
    val docOpenList = ListBuffer.empty[DocumentOpen]
    var docOpen = DocumentOpen.fromString(line._2, line._1)
    while (iter.hasNext && docOpen.nonEmpty) {
      docOpenList += docOpen.get
      nextLine = iter.next()
      docOpen = DocumentOpen.fromString(nextLine._2, nextLine._1)
      if (nextLine._1.split("\\s").head == "DOC_OPEN" && docOpen.isEmpty) {
        return None
      }
    }
    Some(nextLine, docOpenList)
  }


  def extractSessionsFromPaths(rdd: RDD[String], logsList: LogsAccumulator): RDD[Option[Session]] = {
    rdd.map { sessionPath =>
      Using.resource(Source.fromFile(sessionPath, config.encoding)) { source =>

        var session: Option[Session] = None
        var sessionStart: Option[SessionStart] = None
        val quickSearches = scala.collection.mutable.Set.empty[QuickSearch]
        val cardSearches = scala.collection.mutable.Set.empty[CardSearch]
        val iter = source.getLines().toList.zipWithIndex.iterator
        var line = iter.next()
        while (iter.hasNext) {
          line._1.split("\\s+")(0) match {

            case "SESSION_START" =>
              val sessionStartTuple = SessionStart.fromString(line._2, line._1, iter)
              if (sessionStartTuple.nonEmpty) {
                sessionStart = Some(sessionStartTuple.get._2)
                line = sessionStartTuple.get._1
              }
              else {
                logsList.add(s"Invalid SESSION_START format. File:$sessionPath, Row: $line\n")
              }

            case "CARD_SEARCH_START" =>
              val csTuple = CardSearch.fromString(line._2, line._1, iter)
              if (csTuple.nonEmpty) {
                cardSearches.add(csTuple.get._2)
                line = csTuple.get._1
              } else {
                logsList.add(s"Invalid CARD_SEARCH format. File:$sessionPath, Row: $line\n")
              }

            case "QS" =>
              val qsTuple = QuickSearch.fromString(line._2, line._1, iter)
              if (qsTuple.nonEmpty) {
                quickSearches.add(qsTuple.get._2)
                line = qsTuple.get._1
              }
              else {
                logsList.add(s"Invalid QS format. File:$sessionPath, Row: $line\\n")
              }
            case "SESSION_END" =>
              logsList.add(s"There is a row after SESSION_END. File:$sessionPath, row:${iter.next()}")
            case _ =>
              logsList.add(s"File $sessionPath has unrecognized row: $line ")
              line = iter.next()
          }
        }
        if (line._1.split("\\s+")(0) == "SESSION_END") {
          val sessionEnd = SessionEnd.fromString(line._2, line._1)

          var isValidSession = true
          if (sessionEnd.isEmpty) {
            logsList.add(s"SESSION_END is not defined. File:$sessionPath")
            isValidSession = false
          }

          if (sessionStart.isEmpty) {
            logsList.add(s"SESSION_START is not defined. File:$sessionPath")
            isValidSession = false
          }

          if (isValidSession)
            session = Some(Session(
              sessionPath, sessionStart.get, sessionEnd.get,
              quickSearches.toSet, cardSearches.toSet))
        }
        session
      }
    }
  }

}
