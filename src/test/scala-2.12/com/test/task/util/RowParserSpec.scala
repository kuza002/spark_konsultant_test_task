package com.test.task.util
import java.sql.Timestamp
import com.test.task.models._
import com.test.task.service.parsers.RowParser
import org.specs2.mutable.Specification

class RowParserSpec extends Specification {

  "RowParser" should {

    "parseSessionStart correctly" in {
      "parse valid SESSION_START line" in {
        val line = "SESSION_START 01.01.2025_12:00:00"
        val result = RowParser.parseSessionStart(1, line)

        result must beSome
        result.get.rowNum must_== 1
        result.get.timestamp must_== Timestamp.valueOf("2025-01-01 12:00:00")
      }

      "return None for invalid line" in {
        val line = "INVALID 01.01.2025_12:00:00"
        RowParser.parseSessionStart(1, line) must beNone
      }

      "return None for invalid timestamp" in {
        val line = "SESSION_START invalid-timestamp"
        RowParser.parseSessionStart(1, line) must beNone
      }

      "parseSessionEnd correctly" in {
        "parse valid SESSION_END line" in {
          val line = "SESSION_END 01.01.2025_12:00:00"
          val result = RowParser.parseSessionEnd(1, line)

          result must beSome
          result.get.rowNum must_== 1
          result.get.timestamp must_== Timestamp.valueOf("2025-01-01 12:00:00")
        }

        "return None for invalid line" in {
          val line = "INVALID 2025-01-01 12:00:00"
          RowParser.parseSessionEnd(1, line) must beNone
        }
      }

      "parseQuickSearch correctly" in {
        "parse valid QuickSearch line" in {
          val line = "QUICK_SEARCH 01.01.2025_12:00:00 {test query}"
          val result = RowParser.parseQuickSearch(1, line)

          result must beSome
          result.get.rowNum must_== 1
          result.get.timestamp must_== Timestamp.valueOf("2025-01-01 12:00:00")
          result.get.query must_== "test query"
        }

        "return None for line with missing parts" in {
          val line = "QUICK_SEARCH 2023-01-01T12:00:00"
          RowParser.parseQuickSearch(1, line) must beNone
        }

        "return None for invalid timestamp" in {
          val line = "QUICK_SEARCH invalid-timestamp {query}"
          RowParser.parseQuickSearch(1, line) must beNone
        }
      }

      "parseCardSearchStart correctly" in {
        "parse valid CARD_SEARCH_START line" in {
          val line = "CARD_SEARCH_START 01.01.2025_12:00:00"
          val result = RowParser.parseCardSearchStart(1, line)

          result must beSome
          result.get.rowNum must_== 1
          result.get.timestamp must_== Timestamp.valueOf("2025-01-01 12:00:00")
        }

        "return None for invalid line" in {
          val line = "INVALID 2023-01-01T12:00:00"
          RowParser.parseCardSearchStart(1, line) must beNone
        }
      }

      "parseCardSearchEnd correctly" in {
        "parse valid CARD_SEARCH_END line" in {
          val line = "CARD_SEARCH_END"
          val result = RowParser.parseCardSearchEnd(1, line)

          result must beSome
          result.get.rowNum must_== 1
        }

        "return None for invalid line" in {
          val line = "INVALID"
          RowParser.parseCardSearchEnd(1, line) must beNone
        }
      }

      "parseCardSearchFilter correctly" in {
        "parse valid filter line" in {
          val line = "$filter value"
          val result = RowParser.parseCardSearchFilter(1, line)

          result must beSome
          result.get.rowNum must_== 1
          result.get.id must_== "$filter"
          result.get.content must_== "value"
        }

        "return None for line without filter" in {
          val line = "filter value"
          RowParser.parseCardSearchFilter(1, line) must beNone
        }
      }

      "parseSearchResult correctly" in {
        "parse valid SearchResult line" in {
          val line = "123 doc1 doc2 doc3"
          val result = RowParser.parseSearchResult(1, line)

          result must beSome
          result.get.rowNum must_== 1
          result.get.id must_== 123L
          result.get.documents must_== Seq("doc1", "doc2", "doc3")
        }

        "return None for line with non-numeric searchId" in {
          val line = "abc doc1 doc2"
          RowParser.parseSearchResult(1, line) must beNone
        }

        "return None for empty line" in {
          val line = ""
          RowParser.parseSearchResult(1, line) must beNone
        }
      }

      "parseDocOpen correctly" in {
        "parse valid DocumentOpen line with all fields" in {
          val line = "DOC_OPEN 01.01.2025_12:00:00 -1288881507 RLAW123_190046"
          val result = RowParser.parseDocOpen(1, line)

          result must beSome
          result.get.rowNum must_== 1
          result.get.searchId must_== -1288881507
          result.get.documentId must_== "RLAW123_190046"
          result.get.timestamp must beSome(Timestamp.valueOf("2025-01-01 12:00:00"))
        }

        "parse valid DocumentOpen line without timestamp" in {
          val line = "DOC_OPEN  -1288881507 RLAW123_190046"
          val result = RowParser.parseDocOpen(1, line)

          result must beSome
          result.get.rowNum must_== 1
          result.get.searchId must_== -1288881507
          result.get.documentId must_== "RLAW123_190046"
          result.get.timestamp must beNone
        }

        "return None for line without searchId" in {
          val line = "DOC_OPEN 01.01.2025_12:00:00   RLAW123_190046"
          RowParser.parseDocOpen(1, line) must beNone
        }

        "return None for line without docId" in {
          val line = "DOC_OPEN 01.01.2025_12:00:00 -1288881507 "
          RowParser.parseDocOpen(1, line) must beNone
        }
      }
    }
  }}