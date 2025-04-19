package com.test.task.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDProducer {

  implicit class RDDProducingOps[A](input: A) {
    def produceRDD[B](produce: A => RDD[B])(implicit spark: SparkSession): RDD[B] = {
      produce(input)
    }
  }

}

case class RDDProducer[A](input: A)
