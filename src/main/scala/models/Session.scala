package models

case class Session(
                    sessionStart: Option[SessionStart],
                    sessionEnd: Option[SessionEnd],
                    quickSearches: Seq[QuickSearch],
                    cardSearches: Seq[CardSearch]
                  )
