package Producer

object Main {
  def main(args: Array[String]): Unit = {
    val logFileParser = new LogFileParser()
    val logFile = "src/main/resources/logfiles.log"
    val logEntries = logFileParser.parseLogFile(logFile)

    val kafkaMessageSender = new KafkaMessageSender("localhost:9092", "logs")
    logEntries.foreach(logEntry => kafkaMessageSender.sendMessage(logEntry))

    kafkaMessageSender.close()
  }
}
