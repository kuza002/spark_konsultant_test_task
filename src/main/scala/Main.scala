// Main.scala

import org.apache.spark.sql.SparkSession
import services.Parser

import java.io.File
import scala.io.Source
import scala.util.Using


object Main {
  private def getListOfFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .map(_.getPath).toList
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SessionAnalysis")
      .master("local[*]")
      .getOrCreate()


    val inputPath = "./src/main/data/"
    val sessionsPathList = getListOfFiles(inputPath)
    val rawLogs = spark.sparkContext.parallelize(sessionsPathList)
    val sessionsList = rawLogs.map(row => Using.resource(Source.fromFile(row, "windows-1251"))(_.mkString)).map(_.split('\n').toList.zipWithIndex).map(Parser.parseSession)

    print(sessionsList.first())
//    sessionsList.foreach(row => println(f"""\n ${row} \n"""))

    //    print(sessionsRDD)
    spark.close()
  }
}
