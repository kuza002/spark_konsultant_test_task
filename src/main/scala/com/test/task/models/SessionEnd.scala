package com.test.task.models

import com.test.task.util.SparkResource.logsList
import com.test.task.util.{LogsIterator, TimestampParser}

import java.sql.Timestamp

case class SessionEnd(rowNum: Int, timestamp: Timestamp)

object SessionEnd {
  def fromIter(iter: LogsIterator): Option[SessionEnd] = {
    try {
      val splitLine = iter.getLine.split("\\s+", 2)
      val timestamp = TimestampParser.parseTimestamp(splitLine(1))

      Some(new SessionEnd(iter.getPosition, timestamp.get))
    }
    catch {
      case _: Exception =>
        logsList.add(s"Invalid SESSION_END format.")
        None
    }
  }
}