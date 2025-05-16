package com.test.task.models

import com.test.task.config.AnalyzerConfig
import com.test.task.util.LogsIterator
import com.test.task.util.SparkResource.logsList
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import scala.io.Source
import scala.util.Using

case class Session(sessionPath: String,
                   sessionStart: SessionStart,
                   sessionEnd: SessionEnd,
                   quickSearches: Seq[QuickSearch],
                   cardSearches: Seq[CardSearch]) {
  override def toString: String = {
    s"""
       |Session details:
       | Path: $sessionPath
       | Start Time: ${sessionStart.timestamp}
       | End Time: ${sessionEnd.timestamp}
       |
       | Quick Searches:
       |${quickSearches.map(q => s"  - Query: ${q.query},\nSearch result: ${q.searchResult}, \n Opened docs: ${q.openedDocuments}").mkString("\n")}
       |
       | Card Searches:
       | ${cardSearches.map(cs => s"Search result: ${cs.searchResult},\nFilters: ${cs.filters}, Opened docs: ${cs.openedDocuments}").mkString("\n")}
       |""".stripMargin
  }
}

object Session {
  val config: AnalyzerConfig = ConfigSource.default.loadOrThrow[AnalyzerConfig]

  def fromPath(path: String): Option[Session] = {
    Using.resource(Source.fromFile(path, config.encoding)) { source =>

      var sessionStart: Option[SessionStart] = None
      var sessionEnd: Option[SessionEnd] = None
      val quickSearches = scala.collection.mutable.Set.empty[QuickSearch]
      val cardSearches = scala.collection.mutable.Set.empty[CardSearch]

      val iter = new LogsIterator(source.getLines())

      try {
        iter.next()
        while (iter.hasNext) {
          val splitLine = iter.getLine.split("\\s+")
          splitLine(0) match {

            case "SESSION_START" =>
              sessionStart = SessionStart.fromIter(iter)
              iter.next()

            case "CARD_SEARCH_START" =>
              cardSearches.add(CardSearch.fromIter(iter).get)

            case "QS" =>
              quickSearches.add(QuickSearch.fromIter(iter))

            case "SESSION_END" =>
              logsList.add(s"There is a row after SESSION_END. File:$path, row:${iter.next()}")
            case _ =>
              logsList.add(s"File $path has unrecognized row: ${iter.getLine} ")
              iter.next()
          }
        }
        if (iter.getLine.split("\\s+")(0) == "SESSION_END")
          sessionEnd = SessionEnd.fromIter(iter)

        Some(Session(path, sessionStart.get, sessionEnd.get,
          quickSearches.toSeq, cardSearches.toSeq))
      }
      catch {
        case e: Exception =>
          logsList.add(s"Error session parse. File: $path\n${e.getMessage}")
          return None
      }
    }
  }
}
