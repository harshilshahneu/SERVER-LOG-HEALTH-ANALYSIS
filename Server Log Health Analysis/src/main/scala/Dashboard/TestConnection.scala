package Dashboard

import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, InsertAllRequest, TableId}
import com.google.cloud.bigquery.TableId.of
import scala.collection.JavaConverters._
import java.nio.file.Paths

object TestConnection {
  def main(args: Array[String]): Unit = {
    //val bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService

    val keyPath = Paths.get("src/main/resources/vigilant-mix-336215-3ab924f62d0f.json").toAbsolutePath.toString
    System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", keyPath)


    // Define the dataset ID and table ID
    val datasetId = "server_health_analysis"
    val tableId = "test-demo"
    val projectId = "fit-mantra-384121"

    val bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService

    //val rows = Seq(
//      Map("Name" -> "value1", "Number" -> 100).asJava,
//      Map("Name" -> "value3", "Number" -> 200).asJava
//    )

    val rows = Seq(
      Map("Name" -> "value1", "Number" -> 100),
      Map("Name" -> "value3", "Number" -> 200)
    ).map(row => InsertAllRequest.RowToInsert.of(row.asJava))

    val insertRequest = InsertAllRequest.newBuilder(TableId.of(datasetId, tableId)).setRows(rows.asJava).build()
    val insertResponse = bigquery.insertAll(insertRequest)
    if (insertResponse.hasErrors) {
      println("Errors occurred while inserting rows")
    } else {
      println(s"Inserted ${rows.length} rows")
    }

//    val insertRequest = InsertAllRequest.newBuilder(TableId.of(datasetId, tableId)).build()
//    val insertResponse = bigquery.insertAll(insertRequest)
//    if (insertResponse.hasErrors) {
//      println("Errors occurred while inserting rows")
//    } else {
//      //println(s"Inserted ${rows.length} rows")
//      println(s"Rows inserted successfully")
//    }
  }
}
