package com.test.task.service

import com.test.task.models.Session
import org.apache.spark.rdd.RDD

object Supplier {

  def printsRows(rdd: RDD[Session]): Unit = {
    rdd
      .foreach(row => println(f"\n$row\n"))
  }

}
