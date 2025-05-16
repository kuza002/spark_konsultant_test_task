package com.test.task.models

import com.test.task.util.SparkResource.logsList
import com.test.task.util.{LogsIterator, TimestampParser}

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer

case class CardSearch(rowNum: Int,
                      timestamp: Timestamp,
                      filters: Seq[String],
                      searchResult: SearchResult,
                      openedDocuments: Seq[DocumentOpen]
                     )

object CardSearch {
  def fromIter(iter: LogsIterator): Option[CardSearch] = {
    try {
      val rowNum = iter.getPosition
      val splitLine = iter.getLine.strip().split("\\s+")
      val timestamp = TimestampParser.parseTimestamp(splitLine(1))
      val filters = ListBuffer.empty[String]

      iter.next()
      while (iter.getLine.strip() != "CARD_SEARCH_END") {
        filters += iter.getLine
        iter.next()
      }
      iter.next()

      val searchResult = SearchResult.fromString(iter.getLine)
      iter.next()

      val docOpenList = DocumentOpen.getSeq(iter)
      Some(new CardSearch(rowNum, timestamp.get, filters, searchResult.get, docOpenList))
    } catch {
      case _: Exception =>
        logsList.add("Invalid CARD_SEARCH format.")
        None
    }
  }
}
