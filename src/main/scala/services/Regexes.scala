package services

object Regexes {
  val isPosOrNegNum = "-?\\d+"
  val isDataBaseNum = "[A-Z]+"
  val isDocNum = "\\d+"
  val isFilterId = "\\$\\d+"
  val isDocId = "\\w+_\\d+"
}
