package com.test.task

import com.test.task.config.LogConfig
import com.test.task.models.DocumentOpen
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

    // First task

    val targetValue = "ACC_45616"

    val count = rdd.flatMap { session =>
      session.cardSearches.flatMap { cardSearch =>
        cardSearch.searchResult.documents.collect {
          case document if document == (targetValue) => 1
        }
      }
    }.sum().toInt

    println(s"Total occurrences of '$targetValue': $count")

    // Second task

    val docOpens: RDD[DocumentOpen] = rdd.flatMap { session =>
      val quickSearchOpens = session.quickSearches.flatMap { qs =>
        qs.openedDocuments match {
          case Some(docs) => docs
          case None => Seq.empty[DocumentOpen]
        }
      }
      val cardSearchOpens = session.cardSearches.flatMap(_.openedDocuments)
      quickSearchOpens ++ cardSearchOpens
    }.filter(_.timestamp.isDefined)

    val docOpenCounts = docOpens.flatMap { docOpen =>
      docOpen.timestamp.map { ts =>
        val date = new Date(ts.getTime).toLocalDate
        ((date, docOpen.documentId), 1)
      }
    }.reduceByKey(_ + _)

    val sortedResults = docOpenCounts
      .map { case ((date, docId), count) => (date, docId, count) }
      .sortBy { case (date, _, count) => (date.toString, -count) }

    val csvLines = sortedResults.map {
      case (date, docId, count) => s"${date.toString},$docId,$count"
    }

    csvLines.saveAsTextFile("output/document_open_stats")

    val sampleResults = sortedResults.take(20)
    sampleResults.foreach { case (date, docId, count) =>
      println(s"${date.toString}\t$docId\t$count")
    }

  }

  private val errorLog = new File(s"${LogConfig.logDirectory}/${LogConfig.parsingErrorsLog}")
  if (errorLog.exists()) {
    println(s"\nWarning: Some lines failed to parse. See details in ${errorLog.getAbsolutePath}")
  }


}
