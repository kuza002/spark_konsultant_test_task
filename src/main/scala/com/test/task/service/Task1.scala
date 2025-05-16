package com.test.task.service

import com.test.task.models.Session
import org.apache.spark.rdd.RDD


object Task1 {
  def count(rdd: RDD[Session], target: String): Int = {
    rdd.flatMap { session =>
      session.cardSearches.flatMap { cardSearch =>
        cardSearch.filters.collect({
          case filter if filter.split("\\s", 2)(1) == target => 1
        })
      }
    }.sum().toInt
  }
}
