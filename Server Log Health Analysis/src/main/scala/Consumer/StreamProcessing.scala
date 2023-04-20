package Consumer

import org.antlr.v4.runtime.atn.SemanticContext.AND
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.SparkSession


case class KafkaMessage(ipAddress: String, dateTime: String, request: String, endpoint: String, protocol: String, status: Int, bytes: Int, referrer: String, userAgent: String, responseTime: Int) extends Serializable

object StreamProcessing {
  def main(args: Array[String]): Unit = {


    // Set the logger level to error
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
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

        // Batch the data into a DataFrame of 500 rows
        val numRows = df.count()
        val numBatches = math.ceil(numRows.toDouble / 500).toInt
        val batches = (0 until numBatches).map { i =>
          val start = i * 500
          val end = math.min(start + 500, numRows).toInt
          df.limit(end - start).filter($"bytes" > 100000 and $"responseTime" > 100000).collect()
        }

        batches.foreach { batch =>
          val message = s"Number of Anomalies - ${batch.length}\n\n" +
            batch.map(row => s"IP Address: ${row.getAs[String]("ipAddress")}\n" +
              s"Time Stamp: ${row.getAs[String]("dateTime")}\n" +
              s"Request: ${row.getAs[String]("request")}\n" +
              s"Endpoint: ${row.getAs[String]("endpoint")}\n" +
              s"Protocol: ${row.getAs[String]("protocol")}\n" +
              s"Status: ${row.getAs[Int]("status")}\n" +
              s"Bytes: ${row.getAs[Int]("bytes")}\n" +
              s"Referrer: ${row.getAs[String]("referrer")}\n" +
              s"User Agent: ${row.getAs[String]("userAgent")}\n" +
              s"Response Time: ${row.getAs[Int]("responseTime")}\n\n").mkString("\n")

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
