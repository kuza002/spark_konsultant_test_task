package com.test.task.service

import com.test.task.models.Session
import org.apache.spark.rdd.RDD

import java.sql.Date
import java.time.LocalDate

object DocumentAnalyzer {
  def countDocumentOccurrences(rdd: RDD[Session], target: String): Int = {
    rdd.flatMap { session =>
      session.cardSearches.flatMap { cardSearch =>
        cardSearch.filters.collect({
          case filter if filter.split("\\s", 2)(1) == target => 1
        })
      }
    }.sum().toInt
  }

  def getDocumentOpenStats(rdd: RDD[Session]): RDD[(LocalDate, String, Int)] = {
    val docOpens = rdd.flatMap { session =>
      session.quickSearches.flatMap { qs =>
        qs.openedDocuments.map { docOpen =>
          if (docOpen.timestamp.isEmpty) docOpen.copy(timestamp = Some(qs.timestamp))
          else docOpen
        }
      }
    }.cache()

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
