package Producer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class LogFileParserSpec extends AnyFlatSpec with Matchers {
  val logFileParser = new LogFileParser()


  "parseLogFile" should "return an empty list for an empty log file" in {
    val logFile = "src/main/resources/empty.log"
    logFileParser.parseLogFile(logFile) should be(empty)
  }

  "LogFileParser" should "parse all the log entries" in {
    val logFile = "src/main/resources/logfiles.log"
    val parsedLogEntries = logFileParser.parseLogFile(logFile)

    parsedLogEntries should not be empty
    assert(parsedLogEntries.length === 1000000)
  }

  it should "handle invalid log entries" in {
    val invalidLogEntry = "192.168.0.1 - - [14/Apr/2023:10:32:11 +0000] \"GET /api/v1/users HTTP/1.1\" 200 1234 \"-\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64)\" invalid_response_time"
    val parsed = logFileParser.parse(logFileParser.logEntry, invalidLogEntry)

    parsed.successful shouldEqual false
  }

  it should "return a list of JSON strings for a non-empty log file" in {
    val logFile = "src/main/resources/logfiles.log"
    val jsonStrings = logFileParser.parseLogFile(logFile)
    jsonStrings should not be empty
    jsonStrings.foreach { jsonString =>
      assert(jsonString.contains("ipAddress"))
      assert(jsonString.contains("dateTime"))
      assert(jsonString.contains("request"))
      assert(jsonString.contains("endpoint"))
      assert(jsonString.contains("protocol"))
      assert(jsonString.contains("status"))
      assert(jsonString.contains("bytes"))
      assert(jsonString.contains("referrer"))
      assert(jsonString.contains("userAgent"))
      assert(jsonString.contains("responseTime"))
    }
  }
}

