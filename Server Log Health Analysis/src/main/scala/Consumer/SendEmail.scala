package Consumer

import java.util.Properties
import javax.mail._
import javax.mail.internet.{InternetAddress, MimeMessage}

object SendEmail {
  def send(anomaly: String): Unit = {
    val fromEmail = "arvindmann307@gmail.com" // Sender
    val password = "" // Key generated
    val toEmail = "bgharshilshah@gmail.com" // Receiver

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
    message.setText(s"An anomaly has been detected: $anomaly")

    println("Sending Alert")
    Transport.send(message)
  }
}
