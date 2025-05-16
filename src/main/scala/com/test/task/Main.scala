package com.test.task

import com.test.task.config.AnalyzerConfig
import com.test.task.util.SparkResource.{logsList, usingSparkSession}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import com.test.task.service.{DataProcessor, Task1, Task2}

import java.io.PrintWriter


object Main extends App {
  val config = ConfigSource.default.loadOrThrow[AnalyzerConfig]
  usingSparkSession(config.sparkSessionName) { implicit spark =>

    val sessions = DataProcessor.process(config.filesPath)

    sessions.foreach(println)

    // Почему если запишу запись логов в DataProcessor логи не записываются?
    val logsWriter = new PrintWriter(config.logsPath)
    logsList.value.toList.foreach(log => logsWriter.println(log))
    logsWriter.close()

    val documentOccurrences = Task1.count(sessions, config.targetValue)
    val documentOpenStats = Task2.count(sessions)

    val outputWriter = new PrintWriter(config.outputPath)
    outputWriter.println(s"Total occurrences of '${config.targetValue}': $documentOccurrences")

    val sampleResults = documentOpenStats.take(1000)
    sampleResults.foreach { case (date, docId, count) =>
      outputWriter.println(s"${date.toString}\t$docId\t$count")
    }
    outputWriter.close()
  }
}
