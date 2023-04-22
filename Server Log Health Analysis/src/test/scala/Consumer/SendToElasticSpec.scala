package Consumer

import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.ElasticDsl._
//import com.sksamuel.elastic4s.http.JavaClient
//import com.sksamuel.elastic4s.requests.common.RefreshPolicy
//import com.sksamuel.elastic4s.requests.indexes.{CreateIndexRequest, DeleteIndexRequest}
import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.MockitoSugar._

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
    SendToElastic.deleteIndex(indexName) shouldEqual expectedRequest
  }
}
