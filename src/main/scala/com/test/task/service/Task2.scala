package com.test.task.service

import com.test.task.models.{Document, Session}
import org.apache.spark.rdd.RDD

import java.sql.Date
import java.time.LocalDate

object Task2 {
  def count(rdd: RDD[Session]): RDD[(LocalDate, Document, Int)] = {
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
        ((date, docOpen.document), 1)
      }
    }.reduceByKey(_ + _)

    docOpenCounts
      .map { case ((date, docId), count) => (date, docId, count) }
      .sortBy { case (date, _, count) => (date.toString, -count) }

  }
}
