package Consumer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


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
          df.limit(end - start).filter($"bytes" > 100).collect()
        }

        println(s"Number of batches: ${batches.length}")
        // Rule-based classification on the data
        val classifiedBatches = batches.map(batch => batch.filter(row => row.getAs[Int]("bytes") > 100))
        println(s"Number of classified batches: ${classifiedBatches.length}")
        // Print the classified data to the console
        classifiedBatches.foreach { batch =>
          println(s"Batch size: ${batch.length}")
//          batch.foreach(row => println(row))
        }

      }
    }

    // Start the streaming context
    ssc.start()

    // Wait for the streaming context to terminate
    ssc.awaitTermination()
  }
}
