package com.test.task.models

import com.test.task.service.parsers.TimestampParser

import java.sql.Timestamp

case class SessionStart(rowNum: Int, timestamp: Timestamp)

object SessionStart {
  def fromString(rowNum: Int, line: String,
                 iter: Iterator[(String, Int)],
                ): Option[((String, Int), SessionStart)] = {
    val splitLine = line.split("\\s+")

    if (splitLine(0) != "SESSION_START" || splitLine.length != 2) {
      return None
    }

    val timestamp = TimestampParser.parseTimestamp(splitLine(1))
    if (timestamp.isEmpty) {
      return None
    }

    Some((iter.next(), new SessionStart(rowNum, timestamp.get)))
  }
}

case class SessionEnd(rowNum: Int, timestamp: Timestamp)

object SessionEnd {
  def fromString(rowNum: Int, line: String): Option[SessionEnd] = {
    val splitLine = line.split("\\s+", 2)
    if (splitLine.length < 2 || splitLine(0) != "SESSION_END")
      None
    else {
      val timestamp = TimestampParser.parseTimestamp(splitLine(1))
      timestamp match {
        case Some(_) =>
          Some(new SessionEnd(rowNum, timestamp.get))
        case _ =>
          None
      }

    }
  }
}