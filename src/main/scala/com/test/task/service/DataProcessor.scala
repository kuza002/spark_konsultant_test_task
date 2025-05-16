package com.test.task.service

import com.test.task.config.AnalyzerConfig
import com.test.task.models.Session
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import java.io.File

object DataProcessor {
  val config: AnalyzerConfig = ConfigSource.default.loadOrThrow[AnalyzerConfig]

  private def getListOfFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .map(_.getPath).toList
  }

  def process(inputPath: String)(implicit spark: SparkSession): RDD[Session] = {

    val paths = spark.sparkContext.parallelize(getListOfFiles(inputPath))
    val sessions = paths.map(path => Session.fromPath(path)).flatMap(_.toSeq).cache()

    sessions
  }
}
