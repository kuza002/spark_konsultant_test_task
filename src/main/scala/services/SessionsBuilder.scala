//package services
//
//import models._
//import org.apache.spark.rdd.RDD
//
//import java.io.PrintWriter
//import scala.collection.mutable
//import scala.collection.mutable.ListBuffer
//
//object SessionsBuilder {
//  def buildSessions(rawLogs: RDD[String]): RDD[Session] = {
//    rawLogs.zipWithIndex()
//      .mapPartitions { partition =>
//        val sessionAccumulator = mutable.ListBuffer.empty[Session]
//        var currentSession: Option[Session] = None
//        var currentQS: Option[QuickSearch] = None
//        var currentCardSearch: Option[CardSearchBuilder] = None
//
//        def finalizeSession(): Unit = {
//          currentSession.foreach { session =>
//            sessionAccumulator += session
//          }
//          currentSession = None
//        }
//
//        def finalizeQS(): Unit = {
//          currentQS.foreach { qs =>
//            currentSession = currentSession.map { s =>
//              s.copy(quickSearches = s.quickSearches :+ qs)
//            }
//          }
//          currentQS = None
//        }
//
//        def finalizeCardSearch(): Unit = {
//          currentCardSearch.foreach { cs =>
//            cs.build().foreach { cardSearch =>
//              currentSession = currentSession.map { s =>
//                s.copy(cardSearches = s.cardSearches :+ cardSearch)
//              }
//            }
//          }
//          currentCardSearch = None
//        }
//
//        partition.foreach { case (line, index) =>
//          println(line)
//          Parser.parseLine(index, line) match {
//            case Some(start: SessionStart) =>
//              finalizeSession()
//              currentSession = Some(Session(
//                sessionStart = Some(start),
//                sessionEnd = None,
//                quickSearches = Seq.empty,
//                cardSearches = Seq.empty
//              ))
//
//            case Some(end: SessionEnd) =>
//              currentSession = currentSession.map(_.copy(sessionEnd = Some(end)))
//              finalizeSession()
//
//            case Some(qs: QuickSearch) =>
//              finalizeQS()
//              finalizeCardSearch()
//              currentQS = Some(qs)
//
//            case Some(sr: SearchResult) =>
//              currentQS = currentQS.map(_.copy(searchResult = Some(sr)))
//              currentCardSearch.foreach(_.withSearchResult(sr))
//
//            case Some(doc: DocumentOpen) =>
//              currentQS = currentQS.map { qs =>
//                qs.copy(openedDocuments = qs.openedDocuments.orElse(Some(Seq.empty)))
//                  .copy(openedDocuments = Some(qs.openedDocuments.getOrElse(Seq.empty) :+ doc))
//              }
//              currentCardSearch.foreach(_.addDocOpen(doc))
//
//            case Some(start: CardSearchStart) =>
//              finalizeQS()
//              finalizeCardSearch()
//              currentCardSearch = Some(new CardSearchBuilder(start))
//
//            case Some(filter: CardSearchFilter) =>
//              currentCardSearch.foreach(_.addFilter(filter))
//
//            case Some(end: CardSearchEnd) =>
//              currentCardSearch.foreach(_.withEnd(end))
//
//            case _ => // Ignore other events
//          }
//        }
//
//        finalizeCardSearch()
//        finalizeQS()
//        finalizeSession()
//        sessionAccumulator.iterator
//      }
//  }
//
//  private class CardSearchBuilder(start: CardSearchStart) {
//    private var filters = ListBuffer.empty[CardSearchFilter]
//    private var searchResult: Option[SearchResult] = None
//    private var docOpens = ListBuffer.empty[DocumentOpen]
//    private var end: Option[CardSearchEnd] = None
//
//    def addFilter(filter: CardSearchFilter): Unit = filters += filter
//    def withSearchResult(sr: SearchResult): Unit = searchResult = Some(sr)
//    def addDocOpen(doc: DocumentOpen): Unit = docOpens += doc
//    def withEnd(e: CardSearchEnd): Unit = end = Some(e)
//
//    def build(): Option[CardSearch] = {
//      for {
//        sr <- searchResult
//        end <- end
//      } yield CardSearch(
//        rowNum = start.rowNum,
//        timestamp = start.timestamp,
//        filters = filters.toSeq,
//        searchResult = sr,
//        openedDocuments = docOpens.toSeq
//      )
//    }
//  }
//}