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
  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-streaming" % "3.2.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.2.0"
)


