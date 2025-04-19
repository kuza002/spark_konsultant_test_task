package com.test.task

import com.test.task.config.LogConfig
import com.test.task.models.DocumentOpen
import com.test.task.service.DocumentAnalyzer
import com.test.task.service.Producer.getRDD
import com.test.task.service.Step.{step0, step1, step2}
import com.test.task.util.RDDProcessor.RDDProcessorOps
import com.test.task.util.RDDProducer.RDDProducingOps
import com.test.task.util.SparkResource.usingSparkSession
import org.apache.spark.rdd.RDD

import java.io.File
import java.sql.Date

object Main extends App {

  usingSparkSession("SessionAnalysis") { implicit spark =>

    val inputPath = "./src/main/resources/data/"

    val rdd = inputPath.produceRDD(getRDD)
      .transformRDD(step0)
      .transformRDD(step1)
      .transformRDD(step2)
      .cache()

    val targetValue = "ACC_45616"
    val documentOccurrences = DocumentAnalyzer.countDocumentOccurrences(rdd, targetValue)

    println(s"Total occurrences of '$targetValue': $documentOccurrences")

    // Second task
    val documentOpenStats = DocumentAnalyzer.getDocumentOpenStats(rdd)

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
