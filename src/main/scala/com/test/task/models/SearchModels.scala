package com.test.task.models

import java.sql.Timestamp

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
