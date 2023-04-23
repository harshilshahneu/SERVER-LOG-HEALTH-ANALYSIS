package Consumer

import java.util.Properties
import javax.mail._
import javax.mail.internet.{InternetAddress, MimeMessage}

object SendEmail {
  def send(row: org.apache.spark.sql.Row): Unit = {
    val fromEmail = "sender_email" // Sender
    val password = "put_your_key_here" // Key generated
    val toEmail = "receiver_email" // Receiver
    val formattedAnomaly =
      s"IP Address: ${row.getAs[String]("ipAddress")}\n" +
        s"Time Stamp: ${row.getAs[String]("dateTime")}\n" +
        s"Request: ${row.getAs[String]("request")}\n" +
        s"Endpoint: ${row.getAs[String]("endpoint")}\n" +
        s"Protocol: ${row.getAs[String]("protocol")}\n" +
        s"Status: ${row.getAs[Int]("status")}\n" +
        s"Bytes: ${row.getAs[Int]("bytes")}\n" +
        s"Referrer: ${row.getAs[String]("referrer")}\n" +
        s"User Agent: ${row.getAs[String]("userAgent")}\n" +
        s"Response Time: ${row.getAs[Int]("responseTime")}\n\n"

    val props = new Properties()
    props.put("mail.smtp.host", "smtp.gmail.com")
    props.put("mail.smtp.port", "587")
    props.put("mail.smtp.auth", "true")
    props.put("mail.smtp.starttls.enable", "true")
    props.put("mail.smtp.ssl.trust", "smtp.gmail.com")

    val auth = new Authenticator() {
      override def getPasswordAuthentication(): PasswordAuthentication = {
        new PasswordAuthentication(fromEmail, password)
      }
    }

    val session = Session.getInstance(props, auth)

    val message = new MimeMessage(session)
    message.setFrom(new InternetAddress(fromEmail))
    message.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmail))
    message.setSubject("Anomaly Detected")
    message.setText(s"An anomaly has been detected: $formattedAnomaly")

    Transport.send(message)
  }
}