package com.test.task.service.parsers

import java.sql.Timestamp
import java.time.{LocalDateTime, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Locale

object TimestampParser {
  private case class TimestampFormat(formatter: DateTimeFormatter, isZoned: Boolean)

  private val formats = List(
    TimestampFormat(DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss"), isZoned = false),
    TimestampFormat(DateTimeFormatter.ofPattern("EEE,_dd_MMM_yyyy_HH:mm:ss_xxxx", Locale.ENGLISH), isZoned = true),
    TimestampFormat(DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ss_xxxx", Locale.ENGLISH), isZoned = true)
  )

  def parseTimestamp(timeStr: String): Option[Timestamp] = {
    formats.view.flatMap { fmt =>
      try {
        if (fmt.isZoned)
          Some(Timestamp.from(ZonedDateTime.parse(timeStr, fmt.formatter).toInstant))
        else
          Some(Timestamp.valueOf(LocalDateTime.parse(timeStr, fmt.formatter)))
      } catch {
        case _: Exception => None
      }
    }.headOption
  }

}
