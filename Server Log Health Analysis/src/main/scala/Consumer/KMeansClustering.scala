//package Consumer
//
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka010._
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.json4s._
//import org.json4s.jackson.JsonMethods._
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
//import org.apache.spark.ml.feature.VectorAssembler
//import org.apache.spark.ml.linalg.{Vector, Vectors}
//
//case class KafkaMessage(ipAddress: String, dateTime: String, request: String, endpoint: String, protocol: String, status: Int, bytes: Int, referrer: String, userAgent: String, responseTime: Int) extends Serializable
//
//object KMeansClustering {
//
//  def main(args: Array[String]): Unit = {
//
//    // Set the logger level to error
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    Logger.getLogger("akka").setLevel(Level.ERROR)
//
//    // Create a Spark configuration
//    val conf = new SparkConf()
//      .setAppName("StreamingApp")
//      .setMaster("local[*]")
//
//    // Create a Spark Streaming Context
//    val ssc = new StreamingContext(conf, Seconds(1))
//
//    // Define Kafka parameters
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "localhost:9092",
//      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
//      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
//      "group.id" -> "test-consumer-group",
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (true: java.lang.Boolean)
//    )
//
//    // Define topics to read from
//    val topics = Array("logs")
//
//    // Create a Kafka DStream
//    val kafkaStream = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
//    )
//
//    kafkaStream.foreachRDD { rdd =>
//      if (!rdd.isEmpty) {
//        println("rdd found")
//        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
//        import spark.implicits._
//        val df = rdd.map(record => {
//          val jsonString = record.value()
//          implicit val formats = DefaultFormats
//          val kafkaMessage = parse(jsonString).extract[KafkaMessage]
//          kafkaMessage
//        }).toDF()
//
//        // Extract relevant features from the log data
//        val featureCols = Array("status", "bytes", "responseTime")
//        val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
//        val data = assembler.transform(df).select("features")
//
//        // Load KMeans model or train a new one if needed
//        val modelPath = "src/main/resources/logsfiles.log"
//        val kmeans = if (new java.io.File(modelPath).exists) {
//          KMeansModel.load(modelPath)
//        } else {
//          println("Training data...")
//          new KMeans().setK(10).setSeed(1L).fit(data)
//        }
//
//        println("Data Trained, filtering anomalies...")
//        // Add the cluster assignments to the original log data
//        val clusteredData = kmeans.transform(data).join(df)
//        println(clusteredData.count())
//
//        // Identify anomalies as data points that are far from their cluster center
//        val anomalies = clusteredData.filter { row =>
//          val features = row.getAs[Vector]("features")
//          val cluster = row.getAs[Int]("prediction")
//          val centroid = kmeans.clusterCenters(cluster)
//          val distance = Vectors.sqdist(features, centroid)
//          println("distance : " + distance)
//          distance > 100000 // Adjust this threshold as needed
//        }
//
//        // Print out the anomalous log data
////        anomalies.foreach(println)
//
//        println(anomalies.count())
//        if (!anomalies.isEmpty) {
//          val message = "Anomalies detected: \n" + anomalies.collect().mkString("\n")
////          SendEmail.send(message)
//        } else {
//          println("No anomaly detected")
//        }
//
//      } else {
//        // Debug point to check if the RDD is empty
////        println("RDD is empty")
//      }
//    }
//
//    // Start the streaming context
//    ssc.start()
//
//    // Wait for the streaming context to terminate
//    ssc.awaitTermination()
//  }
//}
