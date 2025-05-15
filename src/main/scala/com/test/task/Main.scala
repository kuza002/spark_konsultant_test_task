package com.test.task

import com.test.task.config.AnalyzerConfig
import com.test.task.service.Producer.getRDD
import com.test.task.service.parsers.SessionBuilder
import com.test.task.util.RDDProducer.RDDProducingOps
import com.test.task.util.SparkResource.usingSparkSession
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import com.test.task.service.{DocumentAnalyzer, LogsAccumulator}

import java.io.PrintWriter


object Main extends App {
  val config = ConfigSource.default.loadOrThrow[AnalyzerConfig]
  usingSparkSession(config.sparkSessionName) { implicit spark =>
    val logsList = new LogsAccumulator()
    spark.sparkContext.register(logsList, "logsList")

    val sessionsPathRdd = config.logsPath.produceRDD(getRDD)
    val sessionsRdd = SessionBuilder.extractSessionsFromPaths(sessionsPathRdd, logsList).flatMap(_.toSeq).cache()

    val targetValue = config.targetValue
    val documentOccurrences = DocumentAnalyzer.countDocumentOccurrences(sessionsRdd, targetValue)

    val documentOpenStats = DocumentAnalyzer.getDocumentOpenStats(sessionsRdd)


    val outputWriter = new PrintWriter("./src/main/resources/output/result.txt")
    outputWriter.println(s"Total occurrences of '$targetValue': $documentOccurrences")

    val sampleResults = documentOpenStats.take(1000)
    sampleResults.foreach { case (date, docId, count) =>
      outputWriter.println(s"${date.toString}\t$docId\t$count")
    }
    outputWriter.close()

    val logsWriter = new PrintWriter("./src/main/resources/output/logs.txt")
    logsList.value.toList.foreach(log => logsWriter.println(log))
    logsWriter.close()
  }
}
