package com.test.task.util

import org.apache.spark.rdd.RDD

object RDDProcessor {
  implicit class RDDProcessorOps[A](rdd: RDD[A]) {
    def transformRDD[B](process: RDD[A] => RDD[B]): RDD[B] = process(rdd)
  }
}
