//package services
//
//import models._
//import org.apache.spark.sql.{DataFrame, SparkSession, Dataset}
//import org.apache.spark.sql.functions._
//
//class SessionAnalyzer(spark: SparkSession) {
//  import spark.implicits._
//  import models.EventEncoders._
//
//  def analyzeSessions(events: Dataset[Event]): DataFrame = {
//    // Группировка событий по сессиям
//    val sessions = events
//      .groupBy(window($"timestamp", "5 minutes"), $"eventType")
//      .count()
//      .orderBy("window.start")
//
//    // Анализ поисковых запросов
//    val searchStats = events
//      .filter($"eventType".isin("QS", "CARD_SEARCH"))
//      .groupBy($"eventType")
//      .agg(
//        count("*").as("total_searches"),
//        avg(size(col("documents"))).as("avg_documents_found")
//      )
//
//    // Анализ открытия документов
//    val docStats = events
//      .filter($"eventType" === "DOC_OPEN")
//      .groupBy($"documentId")
//      .count()
//      .orderBy(desc("count"))
//
//    sessions
//  }
//
//  def joinWithSearchInfo(events: Dataset[Event]): DataFrame = {
//    val searches = events.filter(_.eventType == "QS" || _.eventType == "CARD_SEARCH")
//      .asInstanceOf[Dataset[QuickSearch]]
//      .toDF()
//      .withColumnRenamed("searchId", "s_searchId")
//
//    val docOpens = events.filter(_.eventType == "DOC_OPEN")
//      .asInstanceOf[Dataset[DocumentOpen]]
//      .toDF()
//
//    docOpens.join(searches, docOpens("searchId") === searches("s_searchId"))
//  }
//}