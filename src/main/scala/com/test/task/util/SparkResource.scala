package com.test.task.util

import com.test.task.config.SparkConfig
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource
import pureconfig.generic.auto._

object SparkResource {
  val config: SparkConfig = ConfigSource.default.loadOrThrow[SparkConfig]

  def usingSparkSession[T](appName: String)(block: SparkSession => T): T = {
    val spark = SparkSession.builder()
      .appName(appName)
      .master(config.hostName)
      .getOrCreate()

    try {
      block(spark)
    } finally {
      spark.stop()
    }
  }

}
