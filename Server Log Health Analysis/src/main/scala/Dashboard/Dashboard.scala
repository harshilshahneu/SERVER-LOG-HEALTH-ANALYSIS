//package Dashboard
//
//import java.io.FileInputStream
//import com.google.auth.oauth2.GoogleCredentials
//import com.google.auth.oauth2.ServiceAccountCredentials
//import com.google.cloud.bigquery.BigQueryOptions
//import com.google.cloud.bigquery._
//import java.util.UUID
//import scala.collection.JavaConverters._
//import com.google.cloud.bigquery.InsertAllRequest
//import com.google.cloud.bigquery.TableId
//import java.sql.DriverManager
//import java.sql.Connection
//
//object Dashboard extends App {
//
//  val keyPath = "src/main/resources/vigilant-mix-336215-3ab924f62d0f.json"
//
//  // Authenticate with Google Cloud using the service account key file
//  val credentials = ServiceAccountCredentials.fromStream(new FileInputStream(keyPath))
//  val options = BigQueryOptions.newBuilder().setCredentials(credentials).build()
//
//  // Create a BigQuery client
//  val bigquery = options.getService()
//
//  // Define the rows to be inserted
//  val rows = Seq(
//    Seq("John", 30),
//    Seq("Jane", 25),
//    Seq("Bob", 40)
//  )
//
//  case class Employee(Name: String, Number: Int)
//
//  // Create the Employees
//  val employee1 = new Employee("michael", 100000)
//  val employee2 = new Employee("xiangrui", 120000)
//  val employee3 = new Employee("matei", 140000)
//  val employee4 = new Employee("patrick", 160000)
//
//  val df = Seq(employee1, employee2, employee3, employee4).toDF
//
//  // Define the dataset ID and table ID
//  val datasetId = "server_health_analysis"
//  val tableId = "test-demo"
//
//  // Get a reference to the table
//  val table = bigquery.getTable(datasetId, tableId)
//
//  // Define the schema for the table
//  val schema = Schema.of(
//    Field.of("Name", LegacySQLTypeName.STRING),
//    Field.of("Number", LegacySQLTypeName.INTEGER)
//  )
//
//  // Create a new insert request and add rows to it
//  val requestId = java.util.UUID.randomUUID().toString()
//  val requestBuilder = InsertAllRequest.newBuilder(TableId.of(datasetId, tableId))
//  rows.foreach(row => requestBuilder.addRow(requestId, row:_*))
//  val request = requestBuilder.build()
//
//  val response = bigquery.insertAll(request)
//
//  if (response.hasErrors()) {
//    val insertErrors = response.getInsertErrors.asScala
//    println(s"Errors occurred while inserting rows: $insertErrors")
//  } else {
//    println("Rows inserted successfully")
//  }
//}
