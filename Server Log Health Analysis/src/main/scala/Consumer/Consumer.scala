package Consumer

import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import java.util.Properties
import scala.collection.JavaConverters._

case class LogRecord(ip: String, timestamp: String, request: String, responseCode: Int)

object Consumer extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "test-consumer-group")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(List("logs").asJava)

  println("Test")

  while (true) {
    val records: ConsumerRecords[String, String] = consumer.poll(100)
    records.asScala.foreach { record =>
      val jsonString = record.value()
      println(s"Raw JSON string: $jsonString")
      decode[LogRecord](jsonString) match {
        case Left(error) => println(s"Failed to decode JSON: ${error.getMessage}")
        case Right(logRecord) => println("log : " + logRecord)
      }
    }
  }

  consumer.close()
}
