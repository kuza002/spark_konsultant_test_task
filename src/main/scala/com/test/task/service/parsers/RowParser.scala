package com.test.task.service.parsers

import com.test.task.models.{CardSearchEnd, CardSearchFilter, CardSearchStart, DocumentOpen, QuickSearch, SearchResult, SessionEnd, SessionStart}
import com.test.task.util.Regexes

import java.sql.Timestamp

object RowParser {
   def parseSessionStart(rowNum: Int, line: String): Option[SessionStart] = {
    val splitLine = line.split(" ")

    if (splitLine(0) != "SESSION_START") {
      return None
    }

    val timestamp = TimestampParser.parseTimestamp(splitLine(1))
    if (timestamp.isEmpty) {
      return None
    }

    Some(SessionStart(rowNum, timestamp.get))
  }

   def parseSessionEnd(rowNum: Int, line: String): Option[SessionEnd] = {
    val splitLine = line.split("\\s+", 2) // Разделяем по любому количеству пробелов
    if (splitLine.length < 2 || splitLine(0) != "SESSION_END") None
    else {
      TimestampParser.parseTimestamp(splitLine(1)).map(t => SessionEnd(rowNum, t))
    }
  }

   def parseQuickSearch(rowNum: Int, line: String): Option[QuickSearch] = {
    val splitLine = line.split(" ", 3)
    if (splitLine.length < 3) {
      return None
    }

    val timestamp = TimestampParser.parseTimestamp(splitLine(1))
    if (timestamp.isEmpty) {
      return None
    }
    val query = splitLine(2).stripPrefix("{").stripSuffix("}")

    Some(QuickSearch(rowNum, timestamp.get, query, None, None))
  }

   def parseCardSearchStart(rowNum: Int, line: String): Option[CardSearchStart] = {
    val splitLine = line.split(" ")
    if (splitLine(0) != "CARD_SEARCH_START") None
    else {
      val timestamp = TimestampParser.parseTimestamp(splitLine(1))
      timestamp.map(t => CardSearchStart(rowNum, t))
    }
  }

   def parseCardSearchEnd(rowNum: Int, line: String): Option[CardSearchEnd] = {
    if (line.strip() != Regexes.isCardSearchEnd.toString()) None
    else {
      Some(CardSearchEnd(rowNum))
    }
  }

   def parseCardSearchFilter(rowNum: Int, line: String): Option[CardSearchFilter] = {
    val splitLine = line.split(" ", 2)
    if (splitLine.length < 2 || !splitLine(0).startsWith("$")) None
    else {

      Some(CardSearchFilter(rowNum, splitLine(0), splitLine(1)))
    }
  }

   def parseSearchResult(rowNum: Int, line: String): Option[SearchResult] = {
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

   def parseDocOpen(rowNum: Int, line: String): Option[DocumentOpen] = {
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
          TimestampParser.parseTimestamp(word) match {
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
  }
}
