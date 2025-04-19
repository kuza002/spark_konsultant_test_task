package com.test.task.util

import org.apache.spark.rdd.RDD


object RDDSupplier {

  implicit class RDDSupplierOps[A](rdd: RDD[A]) {
    def supply(supplier: RDD[A] => Unit): Unit = {
      supplier(rdd)
    }
  }

}
