package com.test.task.util

import org.apache.spark.sql.SparkSession

object SparkResource {

  def usingSparkSession[T](appName: String)(block: SparkSession => T): T = {
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()

    try {
      block(spark)
    } finally {
      spark.stop()
    }
  }

}
