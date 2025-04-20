package com.test.task.service.parsers

import com.test.task.models._
import com.test.task.util.Regexes


object SessionBuilder {

  private def buildCardSearch(lineSeq: Seq[Row]): Set[CardSearch] = {
    val cardSearchStarts = lineSeq.collect { case cs: CardSearchStart => cs }

    cardSearchStarts.flatMap { startRow =>

      val cardSearchEndOpt = lineSeq.collectFirst {
        case endRow: CardSearchEnd if endRow.rowNum > startRow.rowNum => endRow
      }

      cardSearchEndOpt.flatMap { endRow =>

        val filters = lineSeq.collect {
          case filter: CardSearchFilter
            if filter.rowNum > startRow.rowNum && filter.rowNum < endRow.rowNum => filter
        }

        val searchResultOpt = lineSeq.collectFirst {
          case sr: SearchResult if sr.rowNum > endRow.rowNum => sr
        }

        searchResultOpt.map { searchResult =>

          val docOpens = lineSeq.collect {
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

  private def buildQuickSearch(lineSeq: Seq[Row]): Set[QuickSearch] = {
    lineSeq
      .collect { case qs: QuickSearch => qs }
      .map { qs =>
        val searchResultOpt = lineSeq.collectFirst {
          case sr: SearchResult if sr.rowNum == qs.rowNum + 1 => sr
        }

        val docOpens = searchResultOpt match {
          case Some(sr) =>
            lineSeq
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
                   lineSeq: Seq[Row]): Session = {

    Session(
      sessionPath,
      lineSeq.head.asInstanceOf[SessionStart],
      lineSeq.last.asInstanceOf[SessionEnd],
      buildQuickSearch(lineSeq),
      buildCardSearch(lineSeq)
    )
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
}
