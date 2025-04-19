package com.test.task.models

import java.sql.Timestamp

trait Row {
  def rowType: String
  def rowNum: Int
}
