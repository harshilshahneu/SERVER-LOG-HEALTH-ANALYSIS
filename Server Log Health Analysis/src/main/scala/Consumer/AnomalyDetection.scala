package Consumer

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.OneVsRest
//import org.apache.spark.ml.classification.OneClassSVM
import org.apache.spark.ml.classification.OneVsRestModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AnomalyDetection {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("AnomalyDetection").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("logs")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //stream.map(record => record.value).print()

    // Process each batch of data from Kafka
    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        // Print each record to console
        rdd.foreach(record => println(record.value()))
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

//    // Create a vector assembler to combine response time and time stamp into a feature vector
//    val assembler = new VectorAssembler()
//      .setInputCols(Array("response_time", "timestamp"))
//      .setOutputCol("features")
//  }
//
//  // Define the OCSVM model
//  val ocsModel = new OneVsRest()
//    .setClassifier(new OneClassSVM())
//    .setFeaturesCol("features")
//    .setPredictionCol("prediction")
//
//  // Train the OCSVM model on an empty DataFrame
//  val emptyDataFrame = Seq.empty[(Double, Double)].toDF("response_time", "timestamp")
//  val emptyFeatureVector = assembler.transform(emptyDataFrame)
//  val ocsvmModel = ocsModel.fit(emptyFeatureVector)


