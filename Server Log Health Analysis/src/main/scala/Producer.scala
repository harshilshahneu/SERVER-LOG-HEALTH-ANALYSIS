import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source
import java.util.Properties

object Producer extends App{
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val logFile = "src/main/resources/logfiles.log"
  val source = Source.fromFile(logFile)
  for (line <- source.getLines) {
    val record = new ProducerRecord[String, String]("logs", null, line)
    producer.send(record)
  }

  producer.close()
}
