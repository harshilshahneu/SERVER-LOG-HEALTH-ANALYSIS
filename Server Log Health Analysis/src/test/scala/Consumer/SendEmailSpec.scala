package Consumer

import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types._


class SendEmailSpec extends AnyFlatSpec with Matchers {

  val spark = SparkSession.builder()
    .appName("MyApp")
    .master("local[*]")
    .getOrCreate()

  val schema = StructType(Seq(
    StructField("ipAddress", StringType, nullable = false),
    StructField("dateTime", StringType, nullable = false),
    StructField("request", StringType, nullable = false),
    StructField("endpoint", StringType, nullable = false),
    StructField("protocol", StringType, nullable = false),
    StructField("status", IntegerType, nullable = false),
    StructField("bytes", IntegerType, nullable = false),
    StructField("referrer", StringType, nullable = false),
    StructField("userAgent", StringType, nullable = false),
    StructField("responseTime", IntegerType, nullable = false)
  ))

  val rows = Seq(
    ("127.0.0.1", "27/Dec/2037:12:00:00 +0530", "/login", "GET", "HTTP/1.1", 200, 1024, "", "Mozilla/5.0", 100)
  )
  val rdd = spark.sparkContext.parallelize(rows)
  val rowRdd = rdd.map(r => Row.fromTuple(r))

  val df = spark.createDataFrame(rowRdd, schema)

  "SendEmail" should "send email successfully" in {
    SendEmail.send(df.head)
    // No exception should be thrown
  }
}
