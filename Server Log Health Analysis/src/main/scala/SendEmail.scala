import java.util.Properties
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Authenticator, Message, PasswordAuthentication, Session, Transport}

object SendEmail {
  def main(args: Array[String]): Unit = {
    val fromEmail = "arvindmann307@gmail.com" // replace with your email address
    val password = "@to be replaced with an import" // replace with your password
    val toEmail = "bgharshilshah@gmail.com" // replace with the recipient's email address

    val props = new Properties()
    props.put("mail.smtp.host", "smtp.gmail.com")
    props.put("mail.smtp.port", "587")
    props.put("mail.smtp.auth", "true")
    props.put("mail.smtp.starttls.enable", "true")

    val auth = new Authenticator() {
      override def getPasswordAuthentication(): PasswordAuthentication = {
        new PasswordAuthentication(fromEmail, password)
      }
    }

    val session = Session.getInstance(props, auth)

    val message = new MimeMessage(session)
    message.setFrom(new InternetAddress(fromEmail))
    message.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmail))
    message.setSubject("Test email from Scala")
    message.setText("This is a test email sent from Scala.")

    Transport.send(message)
    println("Email sent successfully.")
  }
}
