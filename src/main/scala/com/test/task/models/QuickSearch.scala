package com.test.task.models

import com.test.task.util.SparkResource.logsList
import com.test.task.util.{LogsIterator, TimestampParser}

import java.sql.Timestamp


case class QuickSearch(rowNum: Int,
                       timestamp: Timestamp,
                       query: String,
                       searchResult: SearchResult,
                       openedDocuments: Seq[DocumentOpen]
                      )

object QuickSearch {

  def fromIter(iter: LogsIterator): QuickSearch = {
    try {
      val rowNum = iter.getPosition
      val splitLine = iter.getLine.split("\\s+", 3)

      val timestamp = TimestampParser.parseTimestamp(splitLine(1))
      val query = splitLine(2).stripPrefix("{").stripSuffix("}")

      iter.next()
      val searchResult = SearchResult.fromString(iter.getLine)

      iter.next()
      val docOpenList = DocumentOpen.getSeq(iter)

      new QuickSearch(rowNum, timestamp.get, query, searchResult.get, docOpenList)
    }
    catch {
      case e: Exception =>
        logsList.add(s"Invalid QS format.")
        throw e
    }
  }
}
