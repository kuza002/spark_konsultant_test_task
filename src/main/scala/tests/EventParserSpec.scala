package tests

//package de.jngold.sparktesttask
//package tests
//
//import org.scalatest.funsuite.AnyFunSuite
//import services.EventParser
//import models.CardSearchStart
//
//import org.json4s.reflect.fail
//
//import java.sql.Timestamp
//
//class EventParserSpec extends AnyFunSuite {
//  test("Parse CARD_SEARCH_START with parameters and documents") {
//    val line = "CARD_SEARCH_START 01.07.2020_13:40:50 param1=value1;param2=value2|DOC1 DOC2 DOC3"
//    val result = EventParser.parseLine(line)
//
//    assert(result.isDefined)
//    result.get match {
//      case cs: CardSearchStart =>
//        assert(cs.parameters.length == 2)
//        assert(cs.parameters.exists(p => p.paramId == "param1" && p.paramValue == "value1"))
//        assert(cs.parameters.exists(p => p.paramId == "param2" && p.paramValue == "value2"))
//        assert(cs.documents.toSet == Set("DOC1", "DOC2", "DOC3"))
//      case _ => fail("Expected CardSearchStart")
//    }
//  }
//
//  test("Parse CARD_SEARCH_START with only parameters") {
//    val line = "CARD_SEARCH_START 01.07.2020_13:40:50 param1=value1;param2=value2"
//    val result = EventParser.parseLine(line)
//
//    assert(result.isDefined)
//    result.get match {
//      case cs: CardSearchStart =>
//        assert(cs.parameters.length == 2)
//        assert(cs.documents.isEmpty)
//      case _ => fail("Expected CardSearchStart")
//    }
//  }
//
//  test("Parse invalid CARD_SEARCH_START") {
//    val line1 = "CARD_SEARCH_START 01.07.2020_13:40:50"
//    val line2 = "CARD_SEARCH_START invalid_date param=value"
//
//    assert(EventParser.parseLine(line1).isEmpty)
//    assert(EventParser.parseLine(line2).isEmpty)
//  }
//
//  test("Parse CARD_SEARCH_START with malformed parameters") {
//    val line = "CARD_SEARCH_START 01.07.2020_13:40:50 param1=value1;invalid_param;param2=value2|DOC1"
//    val result = EventParser.parseLine(line)
//
//    assert(result.isDefined)
//    result.get match {
//      case cs: CardSearchStart =>
//        assert(cs.parameters.length == 2) // invalid_param пропущен
//        assert(cs.documents.contains("DOC1"))
//      case _ => fail("Expected CardSearchStart")
//    }
//  }
//}
