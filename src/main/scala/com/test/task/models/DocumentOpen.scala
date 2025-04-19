package com.test.task.models

import java.sql.Timestamp

case class DocumentOpen(rowNum: Int,
                        timestamp: Option[Timestamp],
                        searchId: Long,
                        documentId: String
                       ) extends Row {
  override def rowType: String = "DOC_OPEN"
}