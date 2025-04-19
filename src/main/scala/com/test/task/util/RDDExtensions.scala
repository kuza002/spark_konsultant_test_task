package com.test.task.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDProcessor {
  implicit class RDDProcessorOps[A](rdd: RDD[A]) {
    def transformRDD[B](process: RDD[A] => RDD[B]): RDD[B] = process(rdd)
  }
}

object RDDProducer {

  implicit class RDDProducingOps[A](input: A) {
    def produceRDD[B](produce: A => RDD[B])(implicit spark: SparkSession): RDD[B] = {
      produce(input)
    }
  }

}

case class RDDProducer[A](input: A)

object RDDSupplier {

  implicit class RDDSupplierOps[A](rdd: RDD[A]) {
    def supply(supplier: RDD[A] => Unit): Unit = {
      supplier(rdd)
    }
  }

}
