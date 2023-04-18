package Consumer

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.json.JSONObject
import org.apache.spark.sql.types.{StringType, StructField, StructType, IntegerType}


object AnomalyDetection {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("AnomalyDetection").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "AnomalyDetection",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topics = Array("logs")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //stream.map(record => record.value).print()

//    // Process each batch of data from Kafka
//    stream.foreachRDD { rdd =>
//      if (!rdd.isEmpty()) {
//        // Print each record to console
//        rdd.foreach(record => println(record.value()))
//      }
//    }

    // Process each batch of data from Kafka
    import spark.implicits._
    var buffer = Seq.empty[(String,Int)].toDF("dateTime", "responseTime")
    // var row = Seq.empty[(String,Int)]

    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {

        rdd.foreach(record => {

          // Create a DataFrame from the records
          //val newLogs = rdd.map(record => (record.value())).toDF("log")

          //val newLogs = rdd.map(record => {
          val jsonString = record.value()
          val jsonObj = new JSONObject(jsonString)
          val dateTime = jsonObj.getString("dateTime")
          val responseTime = jsonObj.getInt("responseTime")

          // Debugging
          println(s"dateTime: $dateTime, responseTime: $responseTime")

          // Create an empty DataFrame with two columns "dateTime" and "responseTime"
          val schema = StructType(Seq(
            StructField("dateTime", StringType),
            StructField("responseTime", IntegerType)
          ))

          val row = Seq(Row(dateTime, responseTime))
          println(s"row: $row, count:${row.length}")
          val dataFrame = spark.createDataFrame(spark.sparkContext.parallelize(row),schema)

          //val newLogs = row.toDF("dateTime", "responseTime")
          //val newLogs = spark.createDF(row)



          val rowRDD = spark.sparkContext.parallelize(row)
          val newLogs = spark.createDataFrame(rowRDD, schema)

          //(dateTime, responseTime)
          //}).toDF("dateTime", "responseTime")

          // Add the new DataFrame to the buffer
          buffer = buffer.union(newLogs)

          println("Buffer contents" +buffer.show())
          println("Buffer length" +buffer.count())

          // check if the buffer has reached 100 records
          if (buffer.count() == 100) {

            val logs = buffer.toDF("log")

            println("Buffer contents" + buffer.show())
            println("Buffer length" + buffer.count())

            // clear the buffer
            buffer.filter(!col("dateTime"))
            buffer.filter(!col("responseTime"))
          }


          //        val df = spark.read.json(rdd.map(_.value))
  //
  //        // Calculate mean and standard deviation of response times for this batch
  //        val stats = df.selectExpr("avg(response_time) as mean", "stddev(response_time) as stdDev").first()
  //        val mean = stats.getDouble(0)
  //        val stdDev = stats.getDouble(1)
  //
  //        // Detect anomalies based on threshold of 3 standard deviations away from the mean
  //        val anomalies = df.filter(df.col("response_time").minus(mean).abs.gt(stdDev.times(3)))
  //
  //        // Print anomalies to console
  //        anomalies.show(false)

        })
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

