package services

import models.{CardSearch, CardSearchFilter, DocumentID, DocumentOpen, SearchResult, QuickSearch, SessionEnd, SessionStart}

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer

object EventParser {
  private val timestampFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")

  private def parseTimestamp(timeStr: String): Option[Timestamp] = {
    try {
      val localDateTime = java.time.LocalDateTime.parse(timeStr, timestampFormatter)
      Some(Timestamp.valueOf(localDateTime))
    } catch {
      case _: Exception => None
    }
  }

  private def parseDocumentID(documentID: String): Option[DocumentID] = {
    val splitDocumentID = documentID.split("_")
    if (splitDocumentID.length != 2) {
      None
    } else {
      val baseNum = splitDocumentID(0)
      val docNum = splitDocumentID(1)

      if (baseNum.matches(Regexes.isDataBaseNum) && docNum.matches(Regexes.isDocNum)) {
        Some(DocumentID(baseNum, docNum))
      } else {
        None
      }
    }
  }

  private def parseSearchResult(line: String): Option[SearchResult]= {

    val searchResultStrSplit = line.split(" ")
    val searchId = searchResultStrSplit.head
    val documentsFoundStr = searchResultStrSplit.tail.toList
    val documentsFound = ListBuffer[DocumentID]()

    if (searchResultStrSplit.isEmpty || !searchId.matches(Regexes.isPosOrNegNum)) {
      return (None, Some(s"Invalid search ID format in line: $searchResultLine"))
    }

    // Parse found documents
    for (docIdStr <- documentsFoundStr) {
      parseDocumentID(docIdStr) match {
        case Some(docId) => documentsFound += docId
        case None => return None
      }
    }
    Some(SearchResult(searchId, documentsFound))
  }
  private def parseFilter(filterStr: String): Option[CardSearchFilter] = {
    val splitFilterStr = filterStr.split(" ", 2)

    val filterId = splitFilterStr(0)
    if (!filterId.matches(Regexes.isFilterId) | splitFilterStr.length < 2 ){
      None
    }
    else{
      Some(CardSearchFilter(filterId, splitFilterStr(1)))
    }
  }

  def sessionStartParser(line: String): (Option[SessionStart], Option[String]) = {
    val splitLine = line.split(" ")

    if (splitLine(0) != "SESSION_START"){
      return (None, Some(s"Invalid SESSION_START format: $line"))
    }

    val timestamp = parseTimestamp(splitLine(1))
    if (timestamp.isEmpty){
      return (None, Some(s"Invalid timestamp format: $line"))
    }

    (Some(SessionStart(timestamp.get)), None)
  }

  def sessionEndParser(line: String): (Option[SessionEnd], Option[String]) = {
    val splitLine = line.split(" ")

    if (splitLine(0) != "SESSION_END"){
      return (None, Some(s"Invalid SESSION_END format: $line"))
    }

    val timestamp = parseTimestamp(splitLine(1))
    if (timestamp.isEmpty){
      return (None, Some(s"Invalid timestamp format: $line"))
    }

    (Some(SessionEnd(timestamp.get)), None)
  }

  def docOpenParser(line: String): (Option[DocumentOpen], Option[String]) = {
    val splitLine = line.split(" ")
    if (splitLine.length < 4) {
      return (None, Some(s"Invalid DOC_OPEN format: $line"))
    }

    val timestamp = parseTimestamp(splitLine(1))
    if (timestamp.isEmpty) {
      return (None, Some(s"Invalid time for DOC_OPEN: $line"))
    }

    if (!splitLine(2).matches(Regexes.isPosOrNegNum)) {
      return (None, Some(s"Invalid search id for DOC_OPEN: $line"))
    }
    val searchId = splitLine(2).toLong

    val documentID = parseDocumentID(splitLine(3))
    if (documentID.isEmpty) {
      return (None, Some(s"Invalid document ID format: $line"))
    }

    (Some(DocumentOpen(timestamp.get, searchId, documentID.get)), None)
  }

  def quickSearchParser(line: String, linesIter: Iterator[String]): (Option[QuickSearch], Option[String]) = {
    val splitLine = line.split(" ", 3)
    if (splitLine.length < 3) {
      return (None, Some(s"Invalid QUICK_SEARCH format: $line"))
    }

    val timestamp = parseTimestamp(splitLine(1))
    if (timestamp.isEmpty) {
      return (None, Some(s"Invalid time for QUICK_SEARCH: $line"))
    }

    val query = splitLine(2).stripPrefix("{").stripSuffix("}")

    val searchResultLine = linesIter.next()
    val searchResult = parseSearchResult(searchResultLine)

    if (searchResult.isEmpty){
      return (None, Some(s"Missing search results line after: $line"))
    }


    // Parse DOC_OPEN events
    val openedDocuments = ListBuffer[DocumentOpen]()
    var processingDocOpens = true

    while (processingDocOpens && linesIter.hasNext) {
      val peekLine = linesIter.next()
      val peekLineSplit = peekLine.split(" ")

      if (peekLineSplit.nonEmpty && peekLineSplit(0) == "DOC_OPEN") {
        docOpenParser(peekLine) match {
          case (Some(docOpen), _) =>
            if (docOpen.searchId == searchId) {
              openedDocuments += docOpen
            } else {
              return (None, Some(s"DOC_OPEN searchId ${docOpen.searchId} doesn't match QUICK_SEARCH searchId $searchId in line: $peekLine"))
            }
          case (_, Some(error)) => return (None, Some(error))
          case _ => return (None, Some(s"Invalid DOC_OPEN format in line: $peekLine"))
        }
      } else {
        processingDocOpens = false
        // In a real implementation, you might want to push this line back
      }
    }

    (Some(QuickSearch(
      timestamp = timestamp.get,
      query = query,
      searchId = searchId,
      foundDocuments = foundDocuments.toList,
      openedDocuments = openedDocuments.toSeq
    )), None)
  }

  def cardSearchParser(line: String, iter: Iterator[String]):(Option[CardSearch], Option[String]) = {
    val splitLine = line.split(" ")
    val timestamp = parseTimestamp(splitLine(1))

    if (timestamp.isEmpty) {
      return (None, Some(s"Invalid time for CARD_SEARCH: $line"))
    }

    var filters = collection.mutable.ArrayBuffer.empty[CardSearchFilter]
    var lineIter = iter.next()
    while (lineIter != "CARD_SEARCH_END"){
      val filter = parseFilter(line)
      if (filter.isEmpty){
        return (None, Some(s"Invalid filter format $line"))
      }
      filters+=filter.get
      lineIter = iter.next()
    }



  }
}