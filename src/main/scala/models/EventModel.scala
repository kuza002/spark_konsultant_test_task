package models

import java.sql.Timestamp


sealed trait Event extends Row {
  def timestamp: Timestamp

  def isMultilineEvent: Boolean
}

case class SessionStart(rowNum: Option[Long], timestamp: Timestamp) extends Event {
  override def rowType: String = "SESSION_START"

  override def isMultilineEvent: Boolean = false

}

case class SessionEnd(rowNum: Option[Long], timestamp: Timestamp) extends Event {
  override def rowType: String = "SESSION_END"

  override def isMultilineEvent: Boolean = false

}

case class QuickSearch(rowNum: Option[Long],
                       timestamp: Timestamp,
                       query: String,
                       searchResult: Option[SearchResult],
                       openedDocuments: Option[Seq[DocumentOpen]]
                      ) extends Event {
  override def rowType: String = "QS"

  override def isMultilineEvent: Boolean = true
}

case class CardSearch(rowNum: Option[Long],
                      timestamp: Timestamp,
                      filters: Seq[CardSearchFilter],
                      searchResult: SearchResult,
                      openedDocuments: Seq[DocumentOpen]
                     ) extends Event {
  override def rowType: String = "CARD_SEARCH"

  override def isMultilineEvent: Boolean = true
}

case class DocumentOpen(rowNum: Option[Long],
                        timestamp: Timestamp,
                        searchId: Long,
                        documentId: String
                       ) extends Event {
  override def rowType: String = "DOC_OPEN"

  override def isMultilineEvent: Boolean = true
}

case class CardSearchStart(rowNum: Option[Long], timestamp: Timestamp) extends Event {
  override def rowType: String = "CARD_SEARCH_START"

  override def isMultilineEvent: Boolean = true
}

case class CardSearchEnd(rowNum: Option[Long], timestamp: Timestamp) extends Event {
  override def rowType: String = "CARD_SEARCH_END"

  override def isMultilineEvent: Boolean = true
}

