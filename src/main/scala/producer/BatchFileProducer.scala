package producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.io.File
import java.util.Properties
import scala.io.Source
import play.api.libs.json._

object BatchFileProducer {

  case class NewsItem(
                       title: String,
                       link: String,
                       content: String,
                       date: Option[String],
                       source: String
                     )

  implicit val newsFormat: OFormat[NewsItem] = Json.format[NewsItem]

  def main(args: Array[String]): Unit = {

    val directory = "data/news_batches"
    val topic = "spark-news-stream-v2"

    val batchSize = 15
    val delayBetweenFilesMs = 10 * 1000L
    val delaySeconds = delayBetweenFilesMs / 1000

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val files = new File(directory)
      .listFiles()
      .filter(_.getName.endsWith(".json"))
      .sortBy(_.getName.replace("batch_", "").replace(".json", "").toInt)
      .toList

    if (files.isEmpty) {
      println("No batch files found.")
      producer.close()
      sys.exit(1)
    }

    files.zipWithIndex.foreach { case (file, index) =>

      println(s"Starting batches from file: ${file.getName}")

      val source = Source.fromFile(file, "UTF-8")
      val content =
        try source.mkString
        finally source.close()

      val newsList = Json.parse(content).as[List[NewsItem]]

      newsList.grouped(batchSize).foreach { batch =>
        batch.foreach { news =>
          val jsonNews = Json.stringify(Json.toJson(news))
          val record = new ProducerRecord[String, String](
            topic,
            file.getName,
            jsonNews
          )
          producer.send(record)
        }
        producer.flush()
      }

      if (index < files.size - 1) {
        println(s"Waiting ${delaySeconds} seconds before next file...\n")
        Thread.sleep(delayBetweenFilesMs)
      }
    }

    producer.close()
  }
}
