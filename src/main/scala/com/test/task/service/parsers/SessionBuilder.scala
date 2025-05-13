package com.test.task.service.parsers

import com.test.task.config.AnalyzerConfig
import com.test.task.models._
import com.test.task.util.{ErrorLogger, Regexes}
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.util.Using
import pureconfig.ConfigSource
import pureconfig.generic.auto._


object SessionBuilder {
  val config: AnalyzerConfig = ConfigSource.default.loadOrThrow[AnalyzerConfig]

  private def buildCardSearch(lineSeq: Seq[Option[Row]]): Set[CardSearch] = {
    val rowsWithOutNone = lineSeq.flatten

    val cardSearchStarts = rowsWithOutNone.collect { case cs: CardSearchStart => cs }

    cardSearchStarts.flatMap { startRow =>

      val cardSearchEndOpt = rowsWithOutNone.collectFirst {
        case endRow: CardSearchEnd if endRow.rowNum > startRow.rowNum => endRow
      }

      cardSearchEndOpt.flatMap { endRow =>

        val filters = rowsWithOutNone.collect {
          case filter: CardSearchFilter
            if filter.rowNum > startRow.rowNum && filter.rowNum < endRow.rowNum => filter
        }

        val searchResultOpt = rowsWithOutNone.collectFirst {
          case sr: SearchResult if sr.rowNum > endRow.rowNum => sr
        }

        searchResultOpt.map { searchResult =>

          val docOpens = rowsWithOutNone.collect {
            case doc: DocumentOpen if doc.rowNum > searchResult.rowNum => doc
          }.takeWhile(_.searchId == searchResult.id)

          CardSearch(
            rowNum = startRow.rowNum,
            timestamp = startRow.timestamp,
            filters = filters,
            searchResult = searchResult,
            openedDocuments = docOpens
          )
        }
      }
    }.toSet
  }

  private def buildQuickSearch(lineSeq: Seq[Option[Row]]): Set[QuickSearch] = {
    val rowsWithOutNone = lineSeq.flatten
    rowsWithOutNone
      .collect { case qs: QuickSearch => qs }
      .map { qs =>
        val searchResultOpt = rowsWithOutNone.collectFirst {
          case sr: SearchResult if sr.rowNum == qs.rowNum + 1 => sr
        }

        val docOpens = searchResultOpt match {
          case Some(sr) =>
            rowsWithOutNone
              .collect { case doc: DocumentOpen if doc.rowNum > sr.rowNum => doc }
              .takeWhile(_.searchId == sr.id)
              .toList
          case None => Nil
        }

        qs.copy(
          searchResult = searchResultOpt,
          openedDocuments = Some(docOpens)
        )
      }
      .toSet
  }

  def parseSession(sessionPath: String,
                   lineSeq: Seq[Option[Row]]): Option[Session] = {
    val sessionStart = lineSeq.head
    if (sessionStart.isEmpty)
      return None


    val sessionEnd = lineSeq.last
    if (sessionEnd.isEmpty)
      return None


    Option(Session(
      sessionPath,
      sessionStart.get.asInstanceOf[SessionStart],
      sessionEnd.get.asInstanceOf[SessionEnd],
      buildQuickSearch(lineSeq),
      buildCardSearch(lineSeq)
    ))
  }

  def parseLine(line: String, rowNum: Int): Option[Row] = {
    val splitLine = line.split(" ")

    val row = splitLine(0) match {
      case Regexes.isCardSearchStart() =>
        RowParser.parseCardSearchStart(rowNum, line)
      case Regexes.isCardSearchEnd() =>
        RowParser.parseCardSearchEnd(rowNum, line)
      case Regexes.isFilter() =>
        RowParser.parseCardSearchFilter(rowNum, line)
      case Regexes.isStartSession() =>
        RowParser.parseSessionStart(rowNum, line)
      case Regexes.isEndSession() =>
        RowParser.parseSessionEnd(rowNum, line)
      case Regexes.isQuickSearch() =>
        RowParser.parseQuickSearch(rowNum, line)
      case Regexes.isSearchId() =>
        RowParser.parseSearchResult(rowNum, line)
      case Regexes.isDocOpen() =>
        RowParser.parseDocOpen(rowNum, line)
      case _ => None
    }
    row
  }


  def extractSessionsFromPaths(rdd: RDD[String]): RDD[Option[Session]] = {
    rdd.map { sessionPath =>
      Using.resource(Source.fromFile(sessionPath, config.encoding)) { source =>
        SessionBuilder.parseSession(sessionPath, source.getLines().toList.zipWithIndex.map {
          case (line, idx) =>
            var row: Option[Row] = None
            try {
              row = SessionBuilder.parseLine(line, idx)
            }
            catch {
              case _: Exception =>
                ErrorLogger.logError(sessionPath, line, "Unrecognized line")
            }
            row
        })
      }
    }
  }

}
