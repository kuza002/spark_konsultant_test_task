package com.test.task.util

import scala.util.matching.Regex

object Regexes {
  val isStartSession: Regex = "SESSION_START".r
  val isEndSession: Regex = "SESSION_END".r
  val isQuickSearch: Regex = "QS".r
  val isSearchResult: Regex = "-?\\d+".r
  val isDocOpen: Regex = "DOC_OPEN".r
  val isSearchId: Regex = "-?\\d+".r
  val isDocId: Regex = "[\\w|\\d]+_[\\w|\\d]+".r
  val isCardSearchStart: Regex = "CARD_SEARCH_START".r
  val isCardSearchEnd: Regex = "CARD_SEARCH_END".r
  val isFilter: Regex = "\\$\\d+".r
}
