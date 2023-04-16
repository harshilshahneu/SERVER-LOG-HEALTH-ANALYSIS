package Consumer

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.ml.feature.VectorAssembler

object AnomalyDetection {

  def main(args: Array[String]): Unit = {

    // Kafka configuration
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "anomalyDetectionGroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Spark configuration
    val sparkConf = new SparkConf()
      .setAppName("AnomalyDetection")
      .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topics = Array("logs")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // Create a vector assembler to combine response time and time stamp into a feature vector
    val assembler = new VectorAssembler()
      .setInputCols(Array("response_time", "timestamp"))
      .setOutputCol("features")
  }

}
