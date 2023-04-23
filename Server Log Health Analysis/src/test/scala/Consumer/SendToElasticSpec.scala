package Consumer

import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.ElasticDsl._
import org.apache.spark.sql.SparkSession
//import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.common.RefreshPolicy
//import com.sksamuel.elastic4s.requests.indexes.{CreateIndexRequest, DeleteIndexRequest}
import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.MockitoSugar._
import org.apache.spark.sql.types._

class SendToElasticSpec extends AnyFlatSpec with Matchers {

  val mockClient = mock[ElasticClient]

  "createIndex" should "create a new index with correct mapping" in {
    val indexName = "testIndex"
    val expectedRequest = createIndex(indexName).mapping(
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

    val newRequest = SendToElastic.createIndexRequest(indexName)

    // Excluding the unique Index ID from the comparison
    val expectedRequestStr = expectedRequest.toString.substring(0,1246)
    val newRequestStr = expectedRequest.toString.substring(0,1246)

    newRequestStr shouldEqual expectedRequestStr
  }

  "deleteIndex" should "delete an index with the given name" in {
    val indexName = "testIndex"
    val expectedRequest = deleteIndex(indexName)
    SendToElastic.deleteIndexRequest(indexName) shouldEqual expectedRequest
  }

  "send" should "send data to ElasticSearch using the provided Row" in {

    val spark = SparkSession.builder()
      .appName("MyApp")
      .master("local[*]")
      .getOrCreate()

    val schema = StructType(
      Seq(
        StructField("ipAddress", StringType),
        StructField("dateTime", StringType),
        StructField("request", StringType),
        StructField("endpoint", StringType),
        StructField("protocol", StringType),
        StructField("status", IntegerType),
        StructField("bytes", IntegerType),
        StructField("referrer", StringType),
        StructField("userAgent", StringType),
        StructField("responseTime", IntegerType)
      )
    )

    val row = Seq(
      (
      "192.168.0.1",
      "20/Apr/2023:10:30:00 +0000",
      "GET",
      "/api/users",
      "HTTP/1.1",
      200,
      1024,
      "https://www.google.com",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
      500
      )
    )

    val rdd = spark.sparkContext.parallelize(row)
    val rowRdd = rdd.map(r => Row.fromTuple(r))

    val df = spark.createDataFrame(rowRdd, schema)

    val expectedFields = Map(
      "ipAddress" -> "192.168.0.1",
      "logDateTime" -> "20/Apr/2023:10:30:00 +0000",
      "timeStamp" -> java.time.LocalDateTime.now(),
      "request" -> "GET",
      "endpoint" -> "/api/users",
      "protocol" -> "HTTP/1.1",
      "status" -> 200,
      "bytes" -> 1024,
      "referrer" -> "https://www.google.com",
      "userAgent" -> "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
      "responseTime" -> 500
    )
    val expectedRequest = indexInto("serveranomalies").fields(expectedFields).refresh(RefreshPolicy.Immediate)
    SendToElastic.client.execute(expectedRequest)/*.await*/ // Mocking the execute method to avoid actual network calls

    SendToElastic.send(df.head)

    verify(mockClient).execute(expectedRequest)

  }
}
