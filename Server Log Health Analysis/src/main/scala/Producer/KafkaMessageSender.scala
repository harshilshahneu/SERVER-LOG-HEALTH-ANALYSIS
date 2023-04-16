package Producer
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaMessageSender(bootstrapServers: String, topicName: String) {
  private val props = new Properties()
  props.put("bootstrap.servers", bootstrapServers)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  private val producer = new KafkaProducer[String, String](props)

  def sendMessage(message: String): Unit = {
    val record = new ProducerRecord[String, String](topicName, message)
    producer.send(record)
  }

  def close(): Unit = {
    producer.close()
  }
}
