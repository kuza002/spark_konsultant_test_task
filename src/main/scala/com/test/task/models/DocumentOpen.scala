package com.test.task.models

import com.test.task.util.{LogsIterator, TimestampParser}

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer

case class DocumentOpen(rowNum: Int,
                        timestamp: Option[Timestamp],
                        searchId: Int,
                        document: Document
                       ) {
  override def toString: String = {
    s"\n${
      timestamp match {
        case Some(t) => t
        case _ => "No Time"
      }
    } $searchId, $document"
  }
}

object DocumentOpen {
  private def isDocumentOpen(line: String): Boolean = line.split("\\s")(0).strip() == "DOC_OPEN"

  def getSeq(iter: LogsIterator): Seq[DocumentOpen] = {
    val docOpenList = ListBuffer.empty[DocumentOpen]
    while (iter.hasNext && DocumentOpen.isDocumentOpen(iter.getLine)) {
      val docOpen = DocumentOpen.fromIter(iter)
      docOpenList += docOpen
      iter.next()
    }
    docOpenList
  }

  def fromIter(iter: LogsIterator): DocumentOpen = {
    var timestamp: Option[Timestamp] = None
    val rowNum = iter.getPosition
    var splitLine = iter.getLine.split("\\s+")

    var ifNoDate = 1
    splitLine = splitLine.tail
    if (splitLine.length == 3) {
      timestamp = TimestampParser.parseTimestamp(splitLine(0))
      ifNoDate = 0
    }
    val searchId = splitLine(1-ifNoDate).toInt
    val doc = Document.fromString(splitLine(2-ifNoDate))

    try
      new DocumentOpen(rowNum, timestamp, searchId, doc.get)
    catch {
      case e: Exception =>
        throw new IllegalArgumentException(s"${e.getStackTrace.headOption}.\n Missed DOC_OPEN parameter")
    }
  }
}
