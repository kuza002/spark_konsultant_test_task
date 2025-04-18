package services

import models._

import java.sql.Timestamp
import java.time.format.DateTimeFormatter

object Parser {

  private val timestampFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")

  private def parseTimestamp(timeStr: String): Option[Timestamp] = {
    try {
      val localDateTime = java.time.LocalDateTime.parse(timeStr, timestampFormatter)
      Some(Timestamp.valueOf(localDateTime))
    } catch {
      case _: Exception => None
    }
  }

  private def parseSessionStart(rowNum: Long, line: String): Option[SessionStart] = {
    val splitLine = line.split(" ")

    if (splitLine(0) != "SESSION_START") {
      return None
    }

    val timestamp = parseTimestamp(splitLine(1))
    if (timestamp.isEmpty) {
      return None
    }

    Some(SessionStart(Some(rowNum), timestamp.get))
  }

  private def parseSessionEnd(rowNum: Long, line: String): Option[SessionEnd] = {
    val splitLine = line.split("\\s+", 2) // Разделяем по любому количеству пробелов
    if (splitLine.length < 2 || splitLine(0) != "SESSION_END") None
    else {
      parseTimestamp(splitLine(1)).map(t => SessionEnd(Some(rowNum), t))
    }
  }

  private def parseQuickSearch(rowNum: Long, line: String): Option[QuickSearch] = {
    val splitLine = line.split(" ", 3)
    if (splitLine.length < 3) {
      return None
    }

    val timestamp = parseTimestamp(splitLine(1))
    if (timestamp.isEmpty) {
      return None
    }
    val query = splitLine(2).stripPrefix("{").stripSuffix("}")

    Some(QuickSearch(Some(rowNum), timestamp.get, query, None, None))
  }

  private def parseCardSearchStart(rowNum: Long, line: String): Option[CardSearchStart] = {
    val splitLine = line.split(" ")
    if (splitLine(0) != "CARD_SEARCH_START") None
    else {
      val timestamp = parseTimestamp(splitLine(1))
      timestamp.map(t => CardSearchStart(Some(rowNum), t))
    }
  }

  private def parseCardSearchEnd(rowNum: Long, line: String): Option[CardSearchEnd] = {
    val splitLine = line.split(" ", 2) // Разделяем только на 2 части
    if (splitLine.length < 2 || splitLine(0) != "CARD_SEARCH_END") None
    else {
      parseTimestamp(splitLine(1)).map(t => CardSearchEnd(Some(rowNum), t))
    }
  }

  private def parseCardSearchFilter(rowNum: Long, line: String): Option[CardSearchFilter] = {
    val splitLine = line.split(" ", 3)
    if (splitLine.length < 3 || !splitLine(0).startsWith("$")) None
    else {
      val id = splitLine(0).stripPrefix("$").toLong
      Some(CardSearchFilter(Some(rowNum), id, splitLine(2)))
    }
  }

  private def parseSearchResult(rowNum: Long, line: String): Option[SearchResult] = {
    val lineSplit = line.split("\\s+") // Разделяем по любому количеству пробелов
    if (lineSplit.isEmpty) None
    else {
      try {
        val searchId = lineSplit.head.toLong
        val documents = lineSplit.tail.toSeq
        Some(SearchResult(Some(rowNum), searchId, documents))
      } catch {
        case _: NumberFormatException => None
      }
    }
  }

  private def parseDocOpen(rowNum: Long, line: String): Option[DocumentOpen] = {
    val splitLine = line.split(" ")
    if (splitLine.length < 4) {
      return None
    }

    val timestamp = parseTimestamp(splitLine(1))
    if (timestamp.isEmpty) {
      return None
    }

    if (!splitLine(2).matches(Regexes.isSearchId.toString())) {
      return None
    }
    val searchId = splitLine(2).toLong

    val documentID = splitLine(3)
    documentID match{
      case Regexes.isDocId() =>
        Some(DocumentOpen(Some(rowNum), timestamp.get, searchId, documentID))
      case _ =>
        None
    }
  }

  def parseSession(lineList: List[(String, Int)]): List[Option[Row]] = {
    lineList.map { case (line, num) => parseLine(line, num) }
  }

  def parseLine(line: String, rowNum:Int): Option[Row] = {
    val splitLine = line.split(" ")

    splitLine(0) match {
      case "CARD_SEARCH_START" => parseCardSearchStart(rowNum, line)
      case "CARD_SEARCH_END" => parseCardSearchEnd(rowNum, line)
      case _ if splitLine.head.startsWith("$") => parseCardSearchFilter(rowNum, line)
      case Regexes.isStartSession() =>
        parseSessionStart(rowNum, line)
      case Regexes.isEndSession() =>
        parseSessionEnd(rowNum, line)
      case Regexes.isQuickSearch() =>
        parseQuickSearch(rowNum, line)
      case Regexes.isSearchResult() =>
        parseSearchResult(rowNum, line)
      case Regexes.isDocOpen() =>
        parseDocOpen(rowNum, line)
      case _ =>
        None
    }
  }
}
