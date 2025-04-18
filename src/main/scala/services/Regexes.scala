package services

import scala.util.matching.Regex

object Regexes {
  val isFilterId: Regex = "\\$\\d+".r
  val isStartSession: Regex = "SESSION_START".r
  val isEndSession: Regex = "SESSION_END".r
  val isQuickSearch: Regex = "QS".r
  val isSearchResult: Regex = "-?\\d+".r
  val isDocOpen: Regex = "DOC_OPEN".r
  val isSearchId: Regex = "-?\\d+".r
  val isDocId: Regex = "[A-Z]+_\\d+".r
}
