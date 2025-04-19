package com.test.task.service

import com.test.task.models.{Row, Session}
import com.test.task.util.ErrorLogger
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.util.Using

object Step {

  def step0(rdd: RDD[String]): RDD[(String, String, Int)] = {
    rdd.flatMap { sessionPath =>
      Using.resource(Source.fromFile(sessionPath, "windows-1251")) { source =>
        source.getLines().toList.zipWithIndex.map {
          case (line, idx) => (sessionPath, line, idx)
        }
      }
    }
  }

  def step1(rdd: RDD[(String, String, Int)]): RDD[(String, Int, Row)] = {
    rdd.flatMap {
      case (sessionPath, line, idx) =>
        val parsedRow = Parser.parseLine(line, idx).map(row => (sessionPath, idx, row))
        if (parsedRow.isEmpty){
          ErrorLogger.logError(sessionPath, line, "Unrecognized line")
        }
        parsedRow
    }
  }

  def step2(rdd: RDD[(String, Int, Row)]): RDD[Session] = {
    rdd.groupBy(_._1)
      .map {
        case (sessionPath, records) =>
          val sortedRows = records.toList.sortBy(_._2).map(_._3)
          Parser.parseSession(sessionPath, sortedRows)
      }
  }

}
