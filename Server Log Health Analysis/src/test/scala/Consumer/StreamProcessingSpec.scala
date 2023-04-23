import Consumer._
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.http.JavaClient
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterAllConfigMap
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import com.sksamuel.elastic4s.ElasticDsl._

class StreamProcessingSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterAllConfigMap {

  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected: Boolean = true
  var spark: SparkSession = _
  var client: ElasticClient = ElasticClient(JavaClient(ElasticProperties("http://localhost:9200")))

  override def beforeAll(): Unit = {
    // Create a SparkSession and ElasticClient before running tests
    spark = SparkSession.builder
      .master("local[*]")
      .appName("test")
      .getOrCreate()
    client = ElasticClient(JavaClient(ElasticProperties("http://localhost:9200")))
  }

  override def afterAll(): Unit = {
    // Stop the SparkSession and ElasticClient after running tests
    spark.stop()
    client.close()
  }

  "StreamProcessing" should "send anomalies to ElasticSearch" in {
    // Input Kafka message
    val jsonString = """{"ipAddress":"127.0.0.1","dateTime":"27/Dec/2037:12:00:00 +0530","request":"GET","endpoint":"/api/test","protocol":"HTTP/1.1","status":200,"bytes":200000,"referrer":"-","userAgent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36","responseTime":80000}"""
    val rdd = spark.sparkContext.parallelize(Seq(jsonString))
    val df = spark.read.json(rdd)

    // Call StreamProcessing.anomalyDF()
    val resultDF = StreamProcessing.classifyAnomalies(df)

    // Check that the resulting DataFrame contains the input message
    assert(resultDF.filter(col("bytes") > 100000 || col("responseTime") > 100000).count() === 1)

    // Send anomalies to ElasticSearch
    resultDF.foreach(row => SendToElastic.send(row))

    // Check that the ElasticSearch index contains the input message
    val searchResult = client.execute {
      search("serveranomalies") query termQuery("ipAddress", "127.0.0.1") limit 1
    }.await.result
    assert(searchResult.hits.hits.head.sourceAsString.contains("127.0.0.1"))
  }
}
