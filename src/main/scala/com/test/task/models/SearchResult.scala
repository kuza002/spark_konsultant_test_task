package com.test.task.models


case class SearchResult(searchId: Long, docOpens: Seq[Document])


object SearchResult {

  def fromString(line: String): Option[SearchResult] = {
    val splitLine = line.split("\\s")
    val searchId = splitLine(0).toLong
    val docOpens = splitLine.tail.map({ line => Document.fromString(line).get })

    Some(SearchResult(searchId, docOpens))
  }
}
