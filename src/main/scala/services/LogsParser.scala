package services

import models._
import org.apache.spark.rdd.RDD

object LogsParser {


  def parseWithErrors(lines: RDD[String]): (RDD[Session], RDD[String]) = {
    val parsed = lines.mapPartitions { iter =>

      val results = collection.mutable.ArrayBuffer.empty[(Option[Event], Option[String])]
      val sessions = collection.mutable.ArrayBuffer.empty[Session]
      var currentSession: Option[Session] = None
      var currentQuickSearches = Seq.empty[QuickSearch]
      var currentCardSearches = Seq.empty[CardSearch]

      while (iter.hasNext) {

        val result = parseSingleLog(iter)
        result._1.foreach {
          case start: SessionStart =>
            // If we have an ongoing session, save it before starting new one
            currentSession.foreach { session =>
              sessions += session.copy(
                quickSearches = currentQuickSearches,
                cardSearches = currentCardSearches
              )
            }
            // Start new session
            currentSession = Some(Session(Some(start), None, Seq.empty, Seq.empty))
            currentQuickSearches = Seq.empty
            currentCardSearches = Seq.empty

          case end: SessionEnd =>
            currentSession = currentSession.map { session =>
              session.copy(
                sessionEnd = Some(end),
                quickSearches = currentQuickSearches,
                cardSearches = currentCardSearches
              )
            }
            // Save completed session
            currentSession.foreach(sessions += _)
            currentSession = None
            currentQuickSearches = Seq.empty
            currentCardSearches = Seq.empty

          case qs: QuickSearch =>
            currentQuickSearches = currentQuickSearches :+ qs

          case cs: CardSearch =>
            currentCardSearches = currentCardSearches :+ cs

          case _ => // Ignore other event types
        }

        results += result
      }

      // Save any remaining incomplete session
      currentSession.foreach { session =>
        sessions += session.copy(
          quickSearches = currentQuickSearches,
          cardSearches = currentCardSearches
        )
      }

      val events = sessions
      val errors = results.flatMap(_._2)
      Iterator((events, errors))
    }

    (parsed.flatMap(_._1), parsed.flatMap(_._2))
  }


  private def parseSingleLog(linesIter: Iterator[String]): (Option[Event], Option[String]) = {
    val line = linesIter.next()
    if (line == null || line.trim.isEmpty) return (None, None)

    val splitLine = line.split(" ")
    splitLine(0) match {
      case "SESSION_START" =>
        EventParser.sessionStartParser(line)

      case "SESSION_END" =>
        EventParser.sessionEndParser(line)

      case "QS" if splitLine.length >= 3 =>
        EventParser.quickSearchParser(line, linesIter)

      case "CARD_SEARCH_START" =>
        EventParser.cardSearchParser(line, linesIter)

      case eventType =>
        (None, Some(s"Unknown event type: $eventType"))
    }

}

}
