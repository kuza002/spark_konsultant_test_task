package com.test.task.util

import com.test.task.config.LogConfig
import java.io.{File, FileWriter, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object ErrorLogger {
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  new File(LogConfig.logDirectory).mkdirs()

  def logError(sessionPath: String, line: String, errorType: String): Unit = {
    if (LogConfig.enableLogging) {
      val timestamp = LocalDateTime.now().format(formatter)
      val logMessage = s"[$timestamp] Error at file $sessionPath: $errorType | Content: '$line'\n"

      val writer = new PrintWriter(new FileWriter(
        s"${LogConfig.logDirectory}/${LogConfig.parsingErrorsLog}",
        true
      ))
      try {
        writer.append(logMessage)
      } finally {
        writer.close()
      }
    }
  }
}