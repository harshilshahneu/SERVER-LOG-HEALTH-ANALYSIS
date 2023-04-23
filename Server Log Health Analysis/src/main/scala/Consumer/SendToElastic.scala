package Consumer

import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.common.RefreshPolicy
import com.sksamuel.elastic4s.requests.indexes.{CreateIndexRequest, DeleteIndexRequest}

object SendToElastic {
    // we must import the dsl
    import com.sksamuel.elastic4s.ElasticDsl._

    // create a client to a local Docker container at localhost:9200
    val client = ElasticClient(JavaClient(ElasticProperties("http://localhost:9200")))

    def createIndexRequest (indexName: String): CreateIndexRequest =
    {
        createIndex(indexName).mapping(
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
    }
    def deleteIndexRequest(indexName: String): DeleteIndexRequest = {
       deleteIndex(indexName)
    }

    def send(row: org.apache.spark.sql.Row): Unit = {
       
        // Convert the timestamp string to a LocalDateTime object
        val formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z")
        val logDateTime = LocalDateTime.parse(row.getAs[String]("dateTime"), formatter)

        //Send data to ElasticSearch
        client.execute {
            indexInto("serveranomalies").fields(
            "ipAddress" -> row.getAs[String]("ipAddress"),
            "ipAddress" -> row.getAs[String]("ipAddress"),
            "logDateTime" -> logDateTime,
            "timeStamp" -> LocalDateTime.now(),
            "request" -> row.getAs[String]("request"),
            "endpoint" -> row.getAs[String]("endpoint"),
            "protocol" -> row.getAs[String]("protocol"),
            "status" -> row.getAs[Int]("status"),
            "bytes" -> row.getAs[Int]("bytes"),
            "referrer" -> row.getAs[String]("referrer"),
            "userAgent" -> row.getAs[String]("userAgent"),
            "responseTime" -> row.getAs[Int]("responseTime")
            ).refresh(RefreshPolicy.Immediate)
        }.await
    }
}
