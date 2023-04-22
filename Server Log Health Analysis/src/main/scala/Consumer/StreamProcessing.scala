package Consumer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.SparkSession
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties }
import com.sksamuel.elastic4s.http.JavaClient


case class KafkaMessage(ipAddress: String, dateTime: String, request: String, endpoint: String, protocol: String, status: Int, bytes: Int, referrer: String, userAgent: String, responseTime: Int) extends Serializable

object StreamProcessing {
  def main(args: Array[String]): Unit = {

    // Set the logger level to error
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // create a client to a local Docker container at localhost:9200
    val client = ElasticClient(JavaClient(ElasticProperties("http://localhost:9200")))

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
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    // Define topics to read from
    val topics = Array("logs")

    // Create a Kafka DStream
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    kafkaStream.foreachRDD { rdd =>
      if (!rdd.isEmpty) {
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        val df = rdd.map(record => {
          val jsonString = record.value()
          implicit val formats = DefaultFormats
          val kafkaMessage = parse(jsonString).extract[KafkaMessage]
          kafkaMessage
        }).toDF()

        val anomalyDF = df.filter($"bytes" > 100000 or $"responseTime" > 100000)
        anomalyDF.collect().foreach { row =>

          //Send anomalies to elastic search
          SendToElastic.send(row)

          //Alert the user
          SendEmail.send(row)
        }
      }
    }

    // Start the streaming context
    ssc.start()

    // Wait for the streaming context to terminate
    ssc.awaitTermination()
  }
}