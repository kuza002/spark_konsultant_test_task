package com.test.task.util

class LogsIterator(iter: Iterator[String], startRowNum: Int = 0, startLine: String = "") extends Iterator[String]{
  private var rowNum: Int = startRowNum
  private var line: String = startLine

  override def hasNext: Boolean = iter.hasNext

  override def next(): String = {
    rowNum +=1
    line = iter.next()
    line
  }

  def getPosition: Int = this.rowNum
  def getLine: String = this.line
}
