package com.test.task.util

import com.test.task.config.SparkConfig
import com.test.task.service.LogsAccumulator
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource
import pureconfig.generic.auto._

object SparkResource {
  val config: SparkConfig = ConfigSource.default.loadOrThrow[SparkConfig]
  val logsList: LogsAccumulator = new LogsAccumulator()


  def usingSparkSession[T](appName: String)(block: SparkSession => T): T = {
    val spark = SparkSession.builder()
      .appName(appName)
      .master(config.hostName)
      .getOrCreate()

    spark.sparkContext.register(logsList, "parsingLogs")

    try {
      block(spark)
    } finally {
      spark.stop()
    }
  }
}
