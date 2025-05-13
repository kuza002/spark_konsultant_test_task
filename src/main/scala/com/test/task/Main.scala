package com.test.task

import com.test.task.config.{AnalyzerConfig, LogConfig}
import com.test.task.service.DocumentAnalyzer
import com.test.task.service.Producer.getRDD
import com.test.task.service.parsers.SessionBuilder
import com.test.task.util.RDDProducer.RDDProducingOps
import com.test.task.util.SparkResource.usingSparkSession
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import java.io.File



object Main extends App {
  val config = ConfigSource.default.loadOrThrow[AnalyzerConfig]
  usingSparkSession(config.sparkSessionName) { implicit spark =>

    val inputPath = config.logsPath

    val sessionsPathRdd = inputPath.produceRDD(getRDD)
    val sessionsRdd = SessionBuilder.extractSessionsFromPaths(sessionsPathRdd).flatMap(_.toSeq).cache()

    val targetValue = config.targetValue
    val documentOccurrences = DocumentAnalyzer.countDocumentOccurrences(sessionsRdd, targetValue)

    println(s"Total occurrences of '$targetValue': $documentOccurrences")

    val documentOpenStats = DocumentAnalyzer.getDocumentOpenStats(sessionsRdd)

    val sampleResults = documentOpenStats.take(20)
    sampleResults.foreach { case (date, docId, count) =>
      println(s"${date.toString}\t$docId\t$count")
    }

  }

  private val errorLog = new File(s"${LogConfig.logDirectory}/${LogConfig.parsingErrorsLog}")
  if (errorLog.exists()) {
    println(s"\nWarning: Some lines failed to parse. See details in ${errorLog.getAbsolutePath}")
  }
}
