import org.apache.spark.sql.SparkSession
import services.LogsParser
import java.io.{File, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import models._

object Main {
  def main(args: Array[String]): Unit = {
    // Инициализация Spark
    val spark = SparkSession.builder()
      .appName("EventProcessing")
      .master("local[*]")
      .config("spark.sql.debug.maxToStringFields", "100")
      .getOrCreate()

    // Настройка вывода
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val outputDir = new File("output")
    if (!outputDir.exists()) outputDir.mkdir()
    val outputFile = new File(s"output/event_processing_output_${timestamp.replace(" ", "_")}.txt")
    val writer = new PrintWriter(outputFile)

    // Заголовок отчета
    writer.println("=" * 80)
    writer.println(s"EVENT PROCESSING REPORT")
    writer.println(s"Generated at: $timestamp")
    writer.println("=" * 80 + "\n")

    // Обработка входных данных
    val inputPath = if (args.length > 0) args(0) else "./src/main/data/0"
    writer.println(s"📂 Input data source: $inputPath\n")
    val rawLogs = spark.sparkContext.textFile(inputPath)

    // Парсинг логов
    writer.println("🔄 Processing log files...")
    val (sessionsRDD, errorsRDD) = LogsParser.parseWithErrors(rawLogs)
    val sessions = sessionsRDD.collect()
    val errors = errorsRDD.collect()

    // Вывод сессий
    writer.println("\n" + "✅ SESSIONS".padTo(80, '='))
    sessions.zipWithIndex.foreach { case (session, index) =>
      writer.println(s"\nSESSION #${index + 1}")
      writer.println("-" * 80)

      // Session start
      session.sessionStart.foreach { start =>
        writer.println(f"${start.eventType}%-15s | ${start.timestamp}%-25s | SESSION START")
      }

      // Quick searches
      if (session.quickSearches.nonEmpty) {
        writer.println("\nQUICK SEARCHES:")
        session.quickSearches.foreach { qs =>
          writer.println(f"${qs.eventType}%-15s | ${qs.timestamp}%-25s | Query: '${qs.query}'")
          writer.println(f"${""}%-15s | ${""}%-25s | Search ID: ${qs.searchId}")
          writer.println(f"${""}%-15s | ${""}%-25s | Found docs: ${qs.foundDocuments.mkString(", ")}")
          if (qs.openedDocuments.nonEmpty) {
            writer.println(f"${""}%-15s | ${""}%-25s | Opened docs:")
            qs.openedDocuments.foreach(doc =>
              writer.println(f"${""}%-15s | ${doc.timestamp}%-25s |   - ${doc.documentId.baseNum}_${doc.documentId.documentNum}")
            )
          }
          writer.println("-" * 80)
        }
      }

      // Card searches
      if (session.cardSearches.nonEmpty) {
        writer.println("\nCARD SEARCHES:")
        session.cardSearches.foreach { cs =>
          writer.println(f"${cs.eventType}%-15s | ${cs.timestamp}%-25s | Search ID: ${cs.searchId}")
          writer.println(f"${""}%-15s | ${""}%-25s | Parameters: ${cs.parameters.mkString(", ")}")
          writer.println(f"${""}%-15s | ${""}%-25s | Documents: ${cs.documents.mkString(", ")}")
          writer.println("-" * 80)
        }
      }

      // Session end
      session.sessionEnd.foreach { end =>
        writer.println(f"${end.eventType}%-15s | ${end.timestamp}%-25s | SESSION END")
      }

      // Session statistics
      writer.println("\nSESSION STATISTICS:")
      writer.println(f"Total quick searches: ${session.quickSearches.size}")
      writer.println(f"Total card searches: ${session.cardSearches.size}")
      writer.println(f"Total documents opened: ${session.quickSearches.flatMap(_.openedDocuments).size}")
      writer.println("-" * 80)
    }

    // Вывод ошибок
    if (errors.nonEmpty) {
      writer.println("\n" + "❌ ERRORS".padTo(80, '='))
      errors.foreach { error =>
        writer.println(error)
      }
    }

    // Статистика
    writer.println("\n" + "📊 STATISTICS".padTo(80, '='))
    val totalEvents = sessions.flatMap(s => s.sessionStart.toSeq ++ s.sessionEnd.toSeq ++
      s.quickSearches ++ s.cardSearches).length + errors.length
    writer.println(f"Total events processed: $totalEvents")
    writer.println(f"Successful sessions: ${sessions.length}")
    writer.println(f"Errors: ${errors.length}")
    writer.println(f"Success rate: ${(totalEvents - errors.length).toDouble / totalEvents * 100}%.2f%%")

    // Закрытие ресурсов
    writer.close()
    spark.stop()

    println(s"✅ Report successfully generated at: ${outputFile.getAbsolutePath}")
  }
}