import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.RDD
import services.EventParser
import org.apache.spark.sql.Row
import java.io.{PrintWriter, File}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._
import java.sql.Timestamp

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EventProcessing")
      .master("local[*]")
      .config("spark.sql.debug.maxToStringFields", "100")
      .getOrCreate()

    import spark.implicits._

    // Создаем файл для вывода с временной меткой в имени
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    val outputDir = new File("output")
    if (!outputDir.exists()) outputDir.mkdir()
    val outputFile = new File(s"output/event_processing_output_$timestamp.txt")
    val writer = new PrintWriter(outputFile)

    try {
      // 1. Чтение данных
      val inputPath = if (args.length > 0) args(0) else "./src/main/data/*"
      writer.println(s"Reading data from: $inputPath")
      val rawLogs = spark.sparkContext.textFile(inputPath)

      // 2. Парсинг событий с сохранением ошибок
      writer.println("\nStarting event parsing...")
      val (eventsRDD, errorsRDD) = EventParser.parseWithErrors(rawLogs)

      // 3. Преобразование в DataFrame
      writer.println("Converting to DataFrame...")
      val eventsDF = EventParser.convertToDataFrame(spark, eventsRDD)

      // 4. Вывод результатов в файл
      writer.println("\n=== Parsed Events Schema ===")
      eventsDF.schema.treeString.linesIterator.foreach(writer.println)

      writer.println("\n=== First 20 Parsed Events ===")
      eventsDF.take(20).foreach { row =>
        writer.println(row.mkString(", "))
      }

      // 5. Вывод ошибок парсинга
      writer.println("\n=== Parsing Errors (first 20) ===")
      errorsRDD.take(20).foreach(writer.println)

      // 6. Статистика
      writer.println("\n=== Processing Statistics ===")
      writer.println(s"Total events parsed: ${eventsRDD.count()}")
      writer.println(s"Total parsing errors: ${errorsRDD.count()}")

      // 7. Решение задач аналитики
      writer.println("\n=== Analytics Results ===")

      // Задача 1: Количество поисков документа ACC_45616 в карточном поиске
      val acc45616SearchCount = eventsDF
        .filter($"eventType" === "CARD_SEARCH")
        .filter(array_contains($"documents", "ACC_45616"))
        .count()

      writer.println(s"\n1. Number of card searches containing document ACC_45616: $acc45616SearchCount")

      // Задача 2: Количество открытий каждого документа из быстрого поиска по дням
      writer.println("\n2. Document opens count from quick search per day:")

      // Сначала находим все документы, найденные через быстрый поиск
      val quickSearchDocsDF = eventsDF
        .filter($"eventType" === "QS")
        .select(explode($"documents").as("document"), $"timestamp")
        .withColumn("date", to_date($"timestamp"))

      // Затем находим все открытия этих документов
      val docOpensDF = eventsDF
        .filter($"eventType" === "DOC_OPEN")
        .select($"documentId", $"timestamp")
        .withColumn("date", to_date($"timestamp"))

      // Соединяем и группируем с явным указанием источника для колонок
      val dailyDocOpens = quickSearchDocsDF.join(docOpensDF,
          quickSearchDocsDF("document") === docOpensDF("documentId"))
        .groupBy(quickSearchDocsDF("date").cast("string").as("date_str"), $"document")
        .agg(count("*").as("open_count"))
        .orderBy($"date_str", $"document")


//      // Сохраняем результаты в файл
//      val dailyOpensOutputPath = s"output/daily_document_opens_$timestamp.csv"
//      dailyDocOpens
//        .coalesce(1)
//        .write
//        .option("header", "true")
//        .csv(dailyOpensOutputPath)
//
//      writer.println(s"Results saved to: $dailyOpensOutputPath")
//
      // Выводим первые 20 строк для примера
      writer.println("\nSample of daily document opens (first 20):")
      dailyDocOpens.take(50).foreach { row =>
        writer.println(s"Date: ${row.getAs[String]("date_str")}, " +
          s"Document: ${row.getAs[String]("document")}, " +
          s"Opens: ${row.getAs[Long]("open_count")}")
      }

    } catch {
      case e: Exception =>
        writer.println(s"\nERROR: ${e.getMessage}")
        e.printStackTrace(writer)
    } finally {
      writer.close()
      spark.stop()
    }
  }
}