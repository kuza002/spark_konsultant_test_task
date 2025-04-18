package models

trait Row {
  def rowType: String

  def rowNum: Option[Long]
}

case class CardSearchFilter(rowNum: Option[Long],
                            id: Long,
                            content: String
                           ) extends Row {
  override def rowType: String = "CARD_SEARCH_FILTER"
}

case class SearchResult(rowNum: Option[Long],
                        id: Long,
                        documents: Seq[String]) extends Row {
  override def rowType: String = "SEARCH_RESULT"
}
