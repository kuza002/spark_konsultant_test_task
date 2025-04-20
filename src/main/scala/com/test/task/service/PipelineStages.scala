package com.test.task.service

import com.test.task.config.AnalyzerConfig
import com.test.task.models.{Row, Session}
import com.test.task.service.parsers.SessionBuilder
import com.test.task.util.ErrorLogger
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.util.Using
import pureconfig.ConfigSource
import pureconfig.generic.auto._


object PipelineStages {
  val config: AnalyzerConfig = ConfigSource.default.loadOrThrow[AnalyzerConfig]

  def extractWithIndexAndPath(rdd: RDD[String]): RDD[(String, String, Int)] = {
    rdd.flatMap { sessionPath =>
      Using.resource(Source.fromFile(sessionPath, config.encoding)) { source =>
        source.getLines().toList.zipWithIndex.map {
          case (line, idx) => (sessionPath, line, idx)
        }
      }
    }
  }

  def parseRows(rdd: RDD[(String, String, Int)]): RDD[(String, Int, Row)] = {
    rdd.flatMap {
      case (sessionPath, line, idx) =>
        try {
          val parsedRow = SessionBuilder.parseLine(line, idx).map(row => (sessionPath, idx, row))
          if (parsedRow.isEmpty) {
            ErrorLogger.logError(sessionPath, line, "Unrecognized line")
          }
          parsedRow
        } catch {
          case _: Exception =>
            ErrorLogger.logError(sessionPath, line, "Unrecognized line")
            None

        }
    }
  }

  def buildSessions(rdd: RDD[(String, Int, Row)]): RDD[Session] = {
    rdd.groupBy(_._1)
      .map {
        case (sessionPath, records) =>
          try {
            val sortedRows = records.toList.sortBy(_._2).map(_._3)
            SessionBuilder.parseSession(sessionPath, sortedRows)
          } catch {
            case _:Exception => ErrorLogger.logError(sessionPath, sessionPath, "Unrecognized line")
              null
          }
      }
  }

}
