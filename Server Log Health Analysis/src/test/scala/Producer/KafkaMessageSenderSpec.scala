import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.Properties
import scala.jdk.CollectionConverters._

import Producer.KafkaMessageSender

class KafkaMessageSenderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  val bootstrapServers = "localhost:9092"
  val topicName = "logs"
  val kafkaSender = new KafkaMessageSender(bootstrapServers, topicName)

  override def afterEach(): Unit = {
    kafkaSender.close()
  }

  "KafkaMessageSender" should "send a message to Kafka" in {
    val message = "test message"
    kafkaSender.sendMessage(message)

    val consumerProps = new Properties()
    consumerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String, String](consumerProps)
    consumer.subscribe(List(topicName).asJava)

    var foundMessage = false
    val pollTimeout = 10000 // 10 seconds
    val pollStartTime = System.currentTimeMillis()
    while (!foundMessage && System.currentTimeMillis() < pollStartTime + pollTimeout) {
      val records = consumer.poll(100)
      records.asScala.foreach { record =>
        if (record.value == message) {
          foundMessage = true
        }
      }
    }
    consumer.close()

    foundMessage shouldBe true
  }
}
