package com.test.task.models

import com.test.task.util.SparkResource.logsList

case class Document(dbId: String, docId: Int)

object Document {

  def fromString(line: String): Option[Document] = {
    try {
      val splitLine = line.split("_")
      val dbId = splitLine(0)
      val docId = splitLine(1).toInt

      Some(Document(dbId, docId))
    }
    catch {
      case _: Exception =>
        logsList.add(s"Invalid document format $line")
        None
    }
  }
}
