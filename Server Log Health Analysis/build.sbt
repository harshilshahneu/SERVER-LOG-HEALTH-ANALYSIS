ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "Server Log Health Analysis"
  )

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "org.apache.kafka" %% "kafka" % "2.8.0",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
  "org.json4s" %% "json4s-jackson" % "3.6.11",
  "org.json4s" %% "json4s-native" % "3.6.11",
  "io.circe" %% "circe-core" % "0.14.1",
  "io.circe" %% "circe-generic" % "0.14.1",
  "io.circe" %% "circe-parser" % "0.14.1",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
    "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-streaming" % "3.2.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.2.0",
  "org.apache.kafka" % "kafka-clients" % "2.8.1",
  "org.apache.spark" %% "spark-mllib" % "3.2.0",
  "javax.mail" % "mail" % "1.4.7",
  "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % "8.6.0",
//  "com.typesafe.akka" %% "akka-actor" % "2.6.10",
//  "com.typesafe.akka" %% "akka-stream" % "2.6.10",
//  "com.google.cloud" % "google-cloud-bigquery" % "2.5.0",
//  "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.30.0",
//  "org.apache.hadoop" % "hadoop-azure-datalake" % "3.3.5",
//    // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-azure
//  "org.apache.hadoop" % "hadoop-azure" % "3.3.5"
//


//  "org.elasticsearch" %% "elasticsearch-spark-30" % "8.6.0"
  //"org.elasticsearch" %% "elasticsearch-spark" % "5.0.0-alpha4"

//  "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % "8.5.2",
//  "com.sksamuel.elastic4s" %% "elastic4s-core" % "8.5.2"



  //"com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.4"
  //"com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop2-1.9.20",
  //"com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.1.12"
  //"com.google.guava" % "guava" % "27.0.1-jre"
)
