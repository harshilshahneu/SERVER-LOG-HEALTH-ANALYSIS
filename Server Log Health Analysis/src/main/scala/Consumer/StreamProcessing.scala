package Consumer

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamProcessing {
  def main(args: Array[String]): Unit = {

    // Create a Spark configuration
    val conf = new SparkConf()
      .setAppName("StreamingApp")
      .setMaster("local[*]")

    // Create a Spark Streaming Context
    val ssc = new StreamingContext(conf, Seconds(1))

    // Define Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Define topics to read from
    val topics = Array("logs")

    // Create a Kafka DStream
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // Process the Kafka stream
    kafkaStream.foreachRDD { rdd =>
      rdd.foreach { record =>
        println(record.value())
      }
    }

    // Start the streaming context
    ssc.start()

    // Wait for the streaming context to terminate
    ssc.awaitTermination()
  }
}
