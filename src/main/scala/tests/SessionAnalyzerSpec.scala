package tests

//package de.jngold.sparktesttask
//package tests
//
//import org.scalatest.funsuite.AnyFunSuite
//import org.apache.spark.sql.SparkSession
//import models.EventModels._
//import services._
//
//class SessionAnalyzerSpec extends AnyFunSuite {
//  val spark = SparkSession.builder()
//    .appName("Test")
//    .master("local[1]")
//    .getOrCreate()
//
//  import spark.implicits._
//
//  test("Count ACC_45616 in card searches") {
//    val testData = Seq(
//      CardSearchStart(new Timestamp(0), Map.empty, Seq("ACC_45616", "LAW_123")),
//      CardSearchStart(new Timestamp(0), Map.empty, Seq("ACC_45615")),
//      CardSearchStart(new Timestamp(0), Map.empty, Seq("ACC_45616"))
//    ).toDS()
//
//    val analyzer = new SessionAnalyzer(spark)
//    assert(analyzer.countCardSearchesForDocument(testData, "ACC_45616") == 2)
//  }
//
//  test("Document opens from quick searches") {
//    val testEvents = Seq(
//      QuickSearch(new Timestamp(0), 1, "test", Seq("DOC1", "DOC2")),
//      DocumentOpen(new Timestamp(0), 1, "DOC1"),
//      DocumentOpen(new Timestamp(0), 1, "DOC2"),
//      DocumentOpen(new Timestamp(0), 1, "DOC1")
//    ).toDS()
//
//    val analyzer = new SessionAnalyzer(spark)
//    val result = analyzer.getDocumentOpensFromQuickSearches(testEvents).collect()
//    assert(result.length == 2)
//    assert(result.contains(("1970-01-01", "DOC1", 2L)))
//  }
//}
