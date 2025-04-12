package models

import java.sql.Timestamp


sealed trait Event {
  def eventType: String
  def timestamp: Timestamp
}

case class SessionStart(timestamp: Timestamp) extends Event {
  override def eventType: String = "SESSION_START"
}

case class SessionEnd(timestamp: Timestamp) extends Event {
  override def eventType: String = "SESSION_END"
}

case class QuickSearch(
                        timestamp: Timestamp,
                        query: String,
                        searchId: Long,
                        documents: Seq[String]
                      ) extends Event {
  override def eventType: String = "QS"
}

case class CardSearch(
                       timestamp: Timestamp,
                       parameters: Map[Int, String],
                       searchId: Long,
                       documents: Seq[String]
                     ) extends Event {
  override def eventType: String = "CARD_SEARCH"
}

case class DocumentOpen(
                         timestamp: Timestamp,
                         searchId: Long,
                         documentId: String
                       ) extends Event {
  override def eventType: String = "DOC_OPEN"
}
