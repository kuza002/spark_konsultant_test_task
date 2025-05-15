package com.test.task.models

case class Session(sessionPath: String,
                   sessionStart: SessionStart,
                   sessionEnd: SessionEnd,
                   quickSearches: Set[QuickSearch],
                   cardSearches: Set[CardSearch]) {
  override def toString: String = {
    s"""
       |Session details:
       | Path: $sessionPath
       | Start Time: ${sessionStart.timestamp}
       | End Time: ${sessionEnd.timestamp}
       |
       | Quick Searches:
       |${quickSearches.map(q => s"  - Query: ${q.query},\nSearch result: ${q.searchResult}, \n Opened docs: ${q.openedDocuments}").mkString("\n")}
       |
       | Card Searches:
       | ${cardSearches.map(cs => s"Search result: ${cs.searchResult},\nFilters: ${cs.filters}, Opened docs: ${cs.openedDocuments}").mkString("\n")}
       |""".stripMargin
  }
}
