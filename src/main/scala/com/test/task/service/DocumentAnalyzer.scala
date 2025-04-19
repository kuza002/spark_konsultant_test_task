package com.test.task.service

import com.test.task.models.{DocumentOpen, Session}
import org.apache.spark.rdd.RDD

import java.sql.Date
import java.time.LocalDate

object DocumentAnalyzer {
  def countDocumentOccurrences(rdd: RDD[Session], target: String): Int = {
    rdd.flatMap { session =>
      session.cardSearches.flatMap { cardSearch =>
        cardSearch.searchResult.documents.collect {
          case document if document == (target) => 1
        }
      }
    }.sum().toInt
  }

  def getDocumentOpenStats(rdd: RDD[Session]): RDD[(LocalDate, String, Int)] = {
    val docOpens: RDD[DocumentOpen] = rdd.flatMap { session =>
      val quickSearchOpens = session.quickSearches.flatMap { qs =>
        qs.openedDocuments match {
          case Some(docs) => docs
          case None => Seq.empty[DocumentOpen]
        }
      }
      val cardSearchOpens = session.cardSearches.flatMap(_.openedDocuments)
      quickSearchOpens ++ cardSearchOpens
    }.filter(_.timestamp.isDefined).cache()

    val docOpenCounts = docOpens.flatMap { docOpen =>
      docOpen.timestamp.map { ts =>
        val date = new Date(ts.getTime).toLocalDate
        ((date, docOpen.documentId), 1)
      }
    }.reduceByKey(_ + _)

    docOpenCounts
      .map { case ((date, docId), count) => (date, docId, count) }
      .sortBy { case (date, _, count) => (date.toString, -count) }

  }
}
