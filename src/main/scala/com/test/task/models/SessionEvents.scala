package com.test.task.models

import java.sql.Timestamp

case class SessionStart(rowNum: Int, timestamp: Timestamp) extends Row {
  override def rowType: String = "SESSION_START"
}

case class SessionEnd(rowNum: Int, timestamp: Timestamp) extends Row {
  override def rowType: String = "SESSION_END"
}
