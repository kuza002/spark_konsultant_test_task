package com.test.task.models

import com.test.task.service.parsers.{SessionBuilder, TimestampParser}

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer


case class QuickSearch(rowNum: Int,
                       timestamp: Timestamp,
                       query: String,
                       searchResult: String,
                       openedDocuments: Seq[DocumentOpen]
                      )

object QuickSearch {

  def fromString(rowNum: Int, line: String,
                 iter: Iterator[(String, Int)]): Option[((String, Int), QuickSearch)] = {
    val splitLine = line.split("\\s+", 3)
    if (splitLine.length < 3 || splitLine(0) != "QS") {
      return None
    }

    val timestamp = TimestampParser.parseTimestamp(splitLine(1))
    if (timestamp.isEmpty) {
      return None
    }
    val query = splitLine(2).stripPrefix("{").stripSuffix("}")

    if (iter.hasNext) {
      var nextLine = iter.next()

      if (!SessionBuilder.isSearchResultId(nextLine._1.split("\\s")(0))) {
        return None
      }

      val searchResult = nextLine._1

      nextLine = iter.next()
      val docOpenListTuple = SessionBuilder.parseDocOpenings(nextLine, iter)
      if (docOpenListTuple.isEmpty)
        return None
      nextLine = docOpenListTuple.get._1
      val docOpenList = docOpenListTuple.get._2

      Some((nextLine, new QuickSearch(rowNum, timestamp.get, query, searchResult, docOpenList)))

    }
    else
      None

  }
}

case class CardSearch(rowNum: Int,
                      timestamp: Timestamp,
                      filters: Seq[String],
                      searchResult: String,
                      openedDocuments: Seq[DocumentOpen]
                     )

object CardSearch {
  def fromString(rowNum: Int, line: String,
                 iter: Iterator[(String, Int)]): Option[((String, Int), CardSearch)] = {
    val splitLine = line.strip().split("\\s+")
    if (splitLine(0) != "CARD_SEARCH_START" || splitLine.length != 2)
      return None

    val timestamp = TimestampParser.parseTimestamp(splitLine(1))

    val filters = ListBuffer.empty[String]
    var curLine = iter.next()
    while (iter.hasNext && curLine._1.startsWith("$") && curLine._1.split("\\s")(0).drop(1).forall(_.isDigit)) {
      filters += curLine._1
      curLine = iter.next()
    }

    if (curLine._1.strip() != "CARD_SEARCH_END")
      return None

    if (iter.hasNext)
      curLine = iter.next()
    else
      return None

    if (!SessionBuilder.isSearchResultId(curLine._1.split("\\s")(0))) {
      return None
    }
    val searchResult = curLine._1

    if (iter.hasNext)
      curLine = iter.next()
    else
      return None

    val docOpenListTuple = SessionBuilder.parseDocOpenings(curLine, iter)
    if (docOpenListTuple.isEmpty)
      return None

    curLine = docOpenListTuple.get._1
    val docOpenList = docOpenListTuple.get._2

    Some((curLine, new CardSearch(rowNum, timestamp.get, filters, searchResult, docOpenList)))

  }
}
