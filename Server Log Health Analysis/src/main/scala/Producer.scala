import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, NoTypeHints}

import java.util.Properties
import scala.io.Source
import scala.util.parsing.combinator.RegexParsers

object Producer extends App with RegexParsers {
  // Define the log format using parser combinators
  def ipAddress: Parser[String] = """\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}""".r //correct
  def dash: Parser[String] = "-" //last
  def dateTime: Parser[String] = """\[(\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})\]""".r ^^ {
    _.replaceAll("\\[", "").replaceAll("\\]", "")
  } //correct
  def request: Parser[String] = """"(POST|GET|PUT|DELETE) """.r ^^ {
    _.replaceAll("\"", "").replaceAll(" ", "")
  } //correct

  def endpoint: Parser[String] = """/?[a-zA-Z0-9/_-]*""".r  //correct

  def protocol: Parser[String] = """HTTP/\d\.\d""".r //correct

  def status: Parser[String] = """\s*"\s*(\d{3})""".r ^^ {
    _.replaceAll("\"", "").trim()
  } //correct
  def bytes: Parser[String] = """\d+""".r //correct

  def referrer: Parser[String] =
    """\s*"(-|http://[a-zA-Z0-9._/-]*)""".r ^^ {
      _.replaceAll("\"", "").trim()
    } //correct
  def userAgent: Parser[String] = """".*"""".r ^^ { str =>
    str.replace("\" \"", "").replace("\"", "")
  }

  def responseTime: Parser[String] = """\d+""".r


  def logEntry: Parser[(String, String, String, String, String, String, String, String, String, String)] =
    ipAddress ~ dash ~ dash ~ dateTime ~ request ~ endpoint ~ protocol ~ status ~ bytes ~ referrer ~ userAgent ~ responseTime ^^ {
      case ip ~ _ ~ _ ~ time ~ req ~ endpoint ~ protocol ~ status ~ bytes ~ referrer ~ ua ~rt =>
        (ip, time, req, endpoint, protocol, status, bytes, referrer, ua, rt)
    }

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  val filename = "logfiles.log"
  val topicName = "logs"
  implicit val formats = DefaultFormats.withHints(NoTypeHints)
  // Read the log file and send each line to Kafka after parsing it as JSON
  val logFile = "src/main/resources/logfiles.log"
  Source.fromFile(logFile).getLines().foreach(line => {
    // Parse the log entry
    val result = parse(logEntry, line)
    result match {
      case Success((ip, time, req, endpoint, protocol, status, bytes, referrer, ua, rt), _) =>
        // Construct a JSON object from the log entry fields

        val json = Serialization.write(
          Map(
            "ip" -> ip,
            "timestamp" -> time,
            "requestType" -> req,
            "endpoint" -> endpoint,
            "protocol" -> protocol,
            "status" -> status.toInt,
            "bytes" -> bytes.toInt,
            "referrer" -> referrer,
            "userAgent" -> ua,
            "responseTime" -> rt
          )
        )
        // Send the JSON object as a Kafka message
        val message = new ProducerRecord[String, String](topicName, json)
        producer.send(message)
      case Failure(msg, _) => println(s"Failed to parse log entry: $msg")
      case Error(msg, _) => println(s"Error while parsing log entry: $msg")
    }
  })
  producer.close()

}
