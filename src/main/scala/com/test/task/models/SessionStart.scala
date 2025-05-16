package com.test.task.models

import com.test.task.util.SparkResource.logsList
import com.test.task.util.{LogsIterator, TimestampParser}

import java.sql.Timestamp

case class SessionStart(rowNum: Int, timestamp: Timestamp)

object SessionStart {
  def fromIter(iter: LogsIterator): Option[SessionStart] = {
    try {
      val splitLine = iter.getLine.split("\\s+")
      val timestamp = TimestampParser.parseTimestamp(splitLine(1))

      Some(new SessionStart(iter.getPosition, timestamp.get))
    }
    catch {
      case _: Exception =>
        logsList.add(s"Invalid SESSION_START format.")
        None
    }
  }
}