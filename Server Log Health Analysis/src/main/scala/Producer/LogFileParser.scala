package Producer

import org.json4s.DefaultFormats
import org.json4s.native.Serialization

import scala.io.Source
import scala.util.parsing.combinator.RegexParsers

case class LogEntry(
  ipAddress: String,
  dateTime: String,
  request: String,
  endpoint: String,
  protocol: String,
  status: Int,
  bytes: Int,
  referrer: String,
  userAgent: String,
  responseTime: Int
)

class LogFileParser extends RegexParsers {

  // Define the log format using parser combinators
  def ipAddress: Parser[String] = """\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}""".r
  def dash: Parser[String] = "-"
  def dateTime: Parser[String] = """\[(\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})\]""".r ^^ {
    _.replaceAll("\\[", "").replaceAll("\\]", "")
  }
  def request: Parser[String] = """"(POST|GET|PUT|DELETE) """.r ^^ {
    _.replaceAll("\"", "").replaceAll(" ", "")
  }
  def endpoint: Parser[String] = """/?[a-zA-Z0-9/_-]*""".r
  def protocol: Parser[String] = """HTTP/\d\.\d""".r
  def status: Parser[Int] = """\s*"\s*(\d{3})""".r ^^ {
    _.replaceAll("\"", "").trim().toInt
  }
  def bytes: Parser[Int] = """\d+""".r ^^ { _.toInt }
  def referrer: Parser[String] =
    """\s*"(-|http://[a-zA-Z0-9._/-]*)""".r ^^ {
      _.replaceAll("\"", "").trim()
    }
  def userAgent: Parser[String] = """".*"""".r ^^ { str =>
    str.replace("\" \"", "").replace("\"", "")
  }
  def responseTime: Parser[Int] = """\d+""".r ^^ { _.toInt }

  def logEntry: Parser[LogEntry] =
    ipAddress ~ dash ~ dash ~ dateTime ~ request ~ endpoint ~ protocol ~ status ~ bytes ~ referrer ~ userAgent ~ responseTime ^^ {
      case ip ~ _ ~ _ ~ time ~ req ~ endpoint ~ protocol ~ status ~ bytes ~ referrer ~ ua ~ rt =>
        LogEntry(ip, time, req, endpoint, protocol, status, bytes, referrer, ua, rt)
    }

  def parseLogFile(logFile: String): Seq[String] = {
    val logEntries = Source.fromFile(logFile).getLines().flatMap { line =>
      parse(logEntry, line) match {
        case Success(logEntry, _) => Some(logEntry)
        case Error(msg, _) => {
          println(s"Error while parsing log entry: $msg")
          None
        }
        case Failure(msg, _) => {
          println(s"Failed to parse log entry: $msg")
          None
        }
      }
    }.toSeq

    // Convert log entries to JSON strings
    implicit val formats: DefaultFormats = DefaultFormats
    logEntries.map(Serialization.write(_))
  }
}
