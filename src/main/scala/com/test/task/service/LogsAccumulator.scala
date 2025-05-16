package com.test.task.service

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ListBuffer

class LogsAccumulator extends AccumulatorV2[String, ListBuffer[String]] {
  private val _list: ListBuffer[String] = ListBuffer.empty[String]

  override def isZero: Boolean = _list.isEmpty

  override def copy(): AccumulatorV2[String, ListBuffer[String]] = {
    val newAcc = new LogsAccumulator()
    newAcc._list ++= _list
    newAcc
  }

  override def reset(): Unit = _list.clear()

  override def add(v: String): Unit = _list += v

  override def merge(other: AccumulatorV2[String, ListBuffer[String]]): Unit = other match {
    case o: LogsAccumulator => _list ++= o._list
  }

  override def value: ListBuffer[String] = _list
}