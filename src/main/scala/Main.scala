import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.RDD
import services.EventParser
import org.apache.spark.sql.Row

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EventProcessing")
      .master("local[*]")
      .config("spark.sql.debug.maxToStringFields", "100")
      .getOrCreate()

    try {
      // 1. Чтение данных
      val inputPath = if (args.length > 0) args(0) else "./src/main/data/*"
      val rawLogs = spark.sparkContext.textFile(inputPath)

      println(s"\n=== Raw Logs (first 20 lines) ===")
      rawLogs.take(20).foreach(println)

      // 2. Парсинг событий с сохранением ошибок
      val (eventsRDD, errorsRDD) = EventParser.parseWithErrors(rawLogs)

      // 3. Преобразование в DataFrame
      val eventsDF = EventParser.convertToDataFrame(spark, eventsRDD)

      // 4. Вывод результатов
      println("\n=== Parsed Events Schema ===")
      eventsDF.printSchema()

      println("\n=== First 20 Parsed Events ===")
      eventsDF.show(20, truncate = false)

      // 5. Вывод ошибок парсинга
      println("\n=== Parsing Errors (first 20) ===")
      errorsRDD.take(20).foreach(println)

      // 6. Сохранение результатов
//      val outputPath = if (args.length > 1) args(1) else "output"
//
//      eventsDF.write.parquet(s"$outputPath/events")
//      errorsRDD.saveAsTextFile(s"$outputPath/errors")
//
//      println(s"\nResults saved to: $outputPath")
//      println(s"  - Parsed events: $outputPath/events")
//      println(s"  - Errors: $outputPath/errors")

    } finally {
      spark.stop()
    }
  }
}