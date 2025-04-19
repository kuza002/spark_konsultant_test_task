package com.test.task.service

import com.test.task.models._
import com.test.task.util.{ErrorLogger, Regexes}

import java.sql.Timestamp
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.{LocalDateTime, ZonedDateTime}
import java.util.Locale

object Parser {
  private case class TimestampFormat(formatter: DateTimeFormatter, isZoned: Boolean)

  private val formats = List(
    TimestampFormat(DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss"), isZoned = false),
    TimestampFormat(DateTimeFormatter.ofPattern("EEE,_dd_MMM_yyyy_HH:mm:ss_xxxx", Locale.ENGLISH), isZoned = true),
    TimestampFormat(DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ss_xxxx", Locale.ENGLISH), isZoned = true)
  )

  def parseTimestamp(timeStr: String): Option[Timestamp] = {
    formats.view.flatMap { fmt =>
      try {
        if (fmt.isZoned)
          Some(Timestamp.from(ZonedDateTime.parse(timeStr, fmt.formatter).toInstant))
        else
          Some(Timestamp.valueOf(LocalDateTime.parse(timeStr, fmt.formatter)))
      } catch {
        case _: Exception => None
      }
    }.headOption
  }


  private def parseSessionStart(rowNum: Int, line: String): Option[SessionStart] = {
    val splitLine = line.split(" ")

    if (splitLine(0) != "SESSION_START") {
      return None
    }

    val timestamp = parseTimestamp(splitLine(1))
    if (timestamp.isEmpty) {
      return None
    }

    Some(SessionStart(rowNum, timestamp.get))
  }

  private def parseSessionEnd(rowNum: Int, line: String): Option[SessionEnd] = {
    val splitLine = line.split("\\s+", 2) // Разделяем по любому количеству пробелов
    if (splitLine.length < 2 || splitLine(0) != "SESSION_END") None
    else {
      parseTimestamp(splitLine(1)).map(t => SessionEnd(rowNum, t))
    }
  }

  private def parseQuickSearch(rowNum: Int, line: String): Option[QuickSearch] = {
    val splitLine = line.split(" ", 3)
    if (splitLine.length < 3) {
      return None
    }

    val timestamp = parseTimestamp(splitLine(1))
    if (timestamp.isEmpty) {
      return None
    }
    val query = splitLine(2).stripPrefix("{").stripSuffix("}")

    Some(QuickSearch(rowNum, timestamp.get, query, None, None))
  }

  private def parseCardSearchStart(rowNum: Int, line: String): Option[CardSearchStart] = {
    val splitLine = line.split(" ")
    if (splitLine(0) != "CARD_SEARCH_START") None
    else {
      val timestamp = parseTimestamp(splitLine(1))
      timestamp.map(t => CardSearchStart(rowNum, t))
    }
  }

  private def parseCardSearchEnd(rowNum: Int, line: String): Option[CardSearchEnd] = {
    if (line.strip() != Regexes.isCardSearchEnd.toString()) None
    else {
      Some(CardSearchEnd(rowNum))
    }
  }

  private def parseCardSearchFilter(rowNum: Int, line: String): Option[CardSearchFilter] = {
    val splitLine = line.split(" ", 2)
    if (splitLine.length < 2 || !splitLine(0).startsWith("$")) None
    else {

      Some(CardSearchFilter(rowNum, splitLine(0), splitLine(1)))
    }
  }

  private def parseSearchResult(rowNum: Int, line: String): Option[SearchResult] = {
    val lineSplit = line.split("\\s+") // Разделяем по любому количеству пробелов
    if (lineSplit.isEmpty) None
    else {
      try {
        val searchId = lineSplit.head.toLong
        val documents = lineSplit.tail.toSeq
        Some(SearchResult(rowNum, searchId, documents))
      } catch {
        case _: NumberFormatException => None
      }
    }
  }

  private def parseDocOpen(rowNum: Int, line: String): Option[DocumentOpen] = {
    val splitLine = line.split("\\s+")

    var searchId: Option[Long] = None
    var docId: Option[String] = None
    var timestamp: Option[Timestamp] = None

    for (word <- splitLine) {
      word match {
        case Regexes.isSearchId() =>
          searchId = Some(word.toLong)
        case Regexes.isDocId() =>
          docId = Some(word)
        case _ =>
          parseTimestamp(word) match {
            case Some(ts) => timestamp = Some(ts)
            case _ => return None
          }
      }
    }

    (searchId, docId) match {
      case (Some(sId), Some(dId)) =>
        Some(DocumentOpen(rowNum, timestamp, sId, dId))
      case _ =>
        None
    }


//    if (splitLine.length < 4) {
//      return None
//    }
//
//    val timestamp = parseTimestamp(splitLine(1))
//
//    if (!splitLine(2).matches(Regexes.isSearchId.toString())) {
//      return None
//    }
//    val searchId = splitLine(2).toLong
//
//    val documentID = splitLine(3)
//    documentID match {
//      case Regexes.isDocId() =>
//        Some(DocumentOpen(rowNum, timestamp, searchId, documentID))
//      case _ =>
//        None
//    }
  }

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
        parseCardSearchStart(rowNum, line)
      case Regexes.isCardSearchEnd() =>
        parseCardSearchEnd(rowNum, line)
      case Regexes.isFilter() =>
        parseCardSearchFilter(rowNum, line)
      case Regexes.isStartSession() =>
        parseSessionStart(rowNum, line)
      case Regexes.isEndSession() =>
        parseSessionEnd(rowNum, line)
      case Regexes.isQuickSearch() =>
        parseQuickSearch(rowNum, line)
      case Regexes.isSearchId() =>
        parseSearchResult(rowNum, line)
      case Regexes.isDocOpen() =>
        parseDocOpen(rowNum, line)
      case _ => None
    }

//    if (row.isEmpty) {
//      ErrorLogger.logError(rowNum, line, "Unrecognized string")
//    }
    row
  }
}
