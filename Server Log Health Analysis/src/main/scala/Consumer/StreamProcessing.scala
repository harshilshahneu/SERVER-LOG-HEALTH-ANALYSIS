package Consumer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.SparkSession
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties }
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.common.RefreshPolicy


case class KafkaMessage(ipAddress: String, dateTime: String, request: String, endpoint: String, protocol: String, status: Int, bytes: Int, referrer: String, userAgent: String, responseTime: Int) extends Serializable

object StreamProcessing {
  def main(args: Array[String]): Unit = {

    // Set the logger level to error
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z")

    // in this example we create a client to a local Docker container at localhost:9200
    val client = ElasticClient(JavaClient(ElasticProperties("http://localhost:9200")))

    // we must import the dsl
    import com.sksamuel.elastic4s.ElasticDsl._
    
    //create an index to store anomalies
    client.execute {
      createIndex("serveranomalies").mapping(
        properties(
          textField("ipAddress"),
          dateField("dateTime"),
          textField("request"),
          textField("endpoint"),
          textField("protocol"),
          intField("status"),
          intField("bytes"),
          textField("referrer"),
          textField("userAgent"),
          intField("responseTime")
        )
      )
    }.await

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
          // Convert the timestamp string to a LocalDateTime object
          val timestamp = LocalDateTime.parse(row.getAs[String]("dateTime"), formatter)

          //Send data to ElasticSearch
           client.execute {
             indexInto("serveranomalies").fields(
               "ipAddress" -> row.getAs[String]("ipAddress"),
               "dateTime" -> timestamp,
               "request" -> row.getAs[String]("request"),
               "endpoint" -> row.getAs[String]("endpoint"),
               "protocol" -> row.getAs[String]("protocol"),
               "status" -> row.getAs[Int]("status"),
               "bytes" -> row.getAs[Int]("bytes"),
               "referrer" -> row.getAs[String]("referrer"),
               "userAgent" -> row.getAs[String]("userAgent"),
               "responseTime" -> row.getAs[Int]("responseTime")
             ).refresh(RefreshPolicy.Immediate)
           }.await

           //Alert the user
           val message = 
               s"IP Address: ${row.getAs[String]("ipAddress")}\n" +
               s"Time Stamp: ${row.getAs[String]("dateTime")}\n" +
               s"Request: ${row.getAs[String]("request")}\n" +
               s"Endpoint: ${row.getAs[String]("endpoint")}\n" +
               s"Protocol: ${row.getAs[String]("protocol")}\n" +
               s"Status: ${row.getAs[Int]("status")}\n" +
               s"Bytes: ${row.getAs[Int]("bytes")}\n" +
               s"Referrer: ${row.getAs[String]("referrer")}\n" +
               s"User Agent: ${row.getAs[String]("userAgent")}\n" +
               s"Response Time: ${row.getAs[Int]("responseTime")}\n\n"

           SendEmail.send(message)
         }
      }
    }

    // Start the streaming context
    ssc.start()

    // Wait for the streaming context to terminate
    ssc.awaitTermination()
  }
}