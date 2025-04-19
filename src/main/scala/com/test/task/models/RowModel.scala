package com.test.task.models

import java.sql.Timestamp

sealed trait Row {
  def rowType: String
  def rowNum: Int
}

case class SessionStart(rowNum: Int, timestamp: Timestamp) extends Row {
  override def rowType: String = "SESSION_START"


}

case class SessionEnd(rowNum: Int, timestamp: Timestamp) extends Row {
  override def rowType: String = "SESSION_END"


}

case class CardSearchFilter(rowNum: Int,
                            id: String,
                            content: String
                           ) extends Row {
  override def rowType: String = "CARD_SEARCH_FILTER"
}

case class SearchResult(rowNum: Int,
                        id: Long,
                        documents: Seq[String]) extends Row {
  override def rowType: String = "SEARCH_RESULT"
}

case class CardSearchStart(rowNum: Int,
                           timestamp: Timestamp) extends Row {
  override def rowType: String = "CARD_SEARCH_START"

}

case class CardSearchEnd(rowNum: Int) extends Row {
  override def rowType: String = "CARD_SEARCH_END"
}

case class QuickSearch(rowNum: Int,
                       timestamp: Timestamp,
                       query: String,
                       searchResult: Option[SearchResult],
                       openedDocuments: Option[Seq[DocumentOpen]]
                      ) extends Row {
  override def rowType: String = "QS"

}

case class CardSearch(rowNum: Int,
                      timestamp: Timestamp,
                      filters: Seq[CardSearchFilter],
                      searchResult: SearchResult,
                      openedDocuments: Seq[DocumentOpen]
                     ) extends Row {
  override def rowType: String = "CARD_SEARCH"

}

case class DocumentOpen(rowNum: Int,
                        timestamp: Option[Timestamp],
                        searchId: Long,
                        documentId: String
                       ) extends Row {
  override def rowType: String = "DOC_OPEN"

}
