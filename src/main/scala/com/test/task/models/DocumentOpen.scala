package com.test.task.models

import com.test.task.service.parsers.{SessionBuilder, TimestampParser}

import java.sql.Timestamp

case class DocumentOpen(rowNum: Int,
                        timestamp: Option[Timestamp],
                        searchId: Long,
                        documentId: String
                       ) {
  override def toString: String = {
    s"\n${
      timestamp match {
        case Some(t) => t
        case _ => "No Time"
      }
    } $searchId, $documentId"
  }
}

object DocumentOpen {
  def fromString(rowNum: Int, line: String): Option[DocumentOpen] = {
    var splitLine = line.split("\\s+")

    if (splitLine(0).strip() != "DOC_OPEN")
      return None

    splitLine = splitLine.tail

    var searchId: Option[Long] = None
    var docId: Option[String] = None
    var timestamp: Option[Timestamp] = None

    splitLine.foreach(word => {
      val curWord = TimestampParser.parseTimestamp(word)
      if (curWord.nonEmpty)
        timestamp = curWord
      else if (SessionBuilder.isSearchResultId(word)) {
        searchId = Some(word.toLong)
      }
      else if (SessionBuilder.isDocId(word)) {
        docId = Some(word)
      }
      else {
        return None
      }
    })

    (searchId, docId) match {
      case (Some(sId), Some(dId)) =>
        Some(new DocumentOpen(rowNum, timestamp, sId, dId))
      case _ =>
        None
    }
  }
}
