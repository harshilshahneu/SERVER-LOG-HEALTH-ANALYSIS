import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, NoTypeHints}
import scala.io.Source
import scala.util.parsing.combinator.RegexParsers

object Producer extends App with RegexParsers {
  // Define the log format using parser combinators
  def ipAddress: Parser[String] = """\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}""".r
  def dash: Parser[String] = "-"
  def dateTime: Parser[String] = """\[\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4}\]""".r
  def requestType: Parser[String] = """"(POST|GET|PUT|DELETE|HEAD) """.r ^^ {
    _.replaceAll("\"", "").replaceAll(" ", "")
  }

  def logEntry: Parser[(String, String, String)] = ipAddress ~ dash ~ dash ~ dateTime ~ requestType ^^ {
    case ip ~ _ ~ _ ~ ts ~ rt => (ip, ts, rt)
  }

  // Configure Kafka producer properties
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  // Create the Kafka producer
  val producer = new KafkaProducer[String, String](props)

  // Set up JSON serialization
  implicit val formats = DefaultFormats.withHints(NoTypeHints)

  // Read the log file and send each line to Kafka after parsing it as JSON
  val logFile = "src/main/resources/logfiles.log"
  val source = Source.fromFile(logFile)
  for (line <- source.getLines) {
    parse(logEntry, line) match {
      case Success((ipAddress, timestamp, requestType), _) =>
        val json = Serialization.write(Map("ip" -> ipAddress, "timestamp" -> timestamp, "requestType" -> requestType))
        val record = new ProducerRecord[String, String]("logs", null, json)
        producer.send(record)
      case _ =>
        println(s"Failed to parse log line")
      // Ignore lines that don't match the log format
    }
  }

  source.close()
  producer.close()
}
