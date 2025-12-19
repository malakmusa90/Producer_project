📰 Batch File Kafka Producer (Scala)

This project sends news items from local JSON batch files into a Kafka topic.
It simulates real-time streaming by sending news in small groups with delays.

📌 What this project does

Reads a big JSON file containing all news (all_news.json)

Splits it into small batch files (each with 15 news items)

Sends batches one by one to a Kafka topic

Adds a short delay between sending each file (simulating streaming)

▶️ Requirements

Scala

sbt

Apache Kafka running locally on:
localhost:9092

Kafka Topic:
spark-news-stream-v2
