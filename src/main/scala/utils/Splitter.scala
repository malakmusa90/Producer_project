package utils

import java.io._
import play.api.libs.json._

object Splitter {

  case class NewsItem(
                       title: String,
                       link: String,
                       content: String,
                       date: Option[String],
                       source: String
                     )

  implicit val format: OFormat[NewsItem] = Json.format[NewsItem]

  def main(args: Array[String]): Unit = {

    val inputPath = "data/all_news.json"
    val outputDir = new File("data/news_batches")

    if (!outputDir.exists()) {
      outputDir.mkdirs()
      println("Created folder: data/news_batches")
    }

    val source = scala.io.Source.fromFile(inputPath, "UTF-8")
    val jsonStr =
      try source.mkString
      finally source.close()

    val allNews = Json.parse(jsonStr).as[List[NewsItem]]

    println(s"Total news loaded: ${allNews.size}")

    val batchSize = 15
    val batches = allNews.grouped(batchSize).toList

    println(s"Creating ${batches.size} batch files (15 news per batch)\n")

    batches.zipWithIndex.foreach { case (batch, index) =>
      val file = new File(outputDir, s"batch_${index + 1}.json")
      val writer = new PrintWriter(file)

      try {
        writer.write(Json.prettyPrint(Json.toJson(batch)))
      } finally {
        writer.close()
      }

      if ((index + 1) % 50 == 0)
        println(s"Created ${index + 1} batches")
    }

    println("\nSplitting completed successfully")
  }
}
