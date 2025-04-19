package com.test.task.models

case class Session(sessionPath: String,
                   sessionStart: SessionStart,
                   sessionEnd: SessionEnd,
                   quickSearches: Set[QuickSearch],
                   cardSearches: Set[CardSearch])
