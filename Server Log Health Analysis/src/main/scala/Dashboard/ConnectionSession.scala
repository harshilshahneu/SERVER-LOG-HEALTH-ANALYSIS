package Dashboard

import org.apache.spark.sql.SparkSession

object ConnectionSession {

  val spark = SparkSession.builder().appName("TestConnection3").master("local").getOrCreate()

  spark.conf.set("fs.adl.oauth2.access.token.provider.type", "ClientCredential")
  spark.conf.set("fs.adl.oauth2.client.id", "1ca514b6-617f-451e-ae3b-9bcf5363e6d6")
  spark.conf.set("fs.adl.oauth2.credential", "qxf8Q~EFd29QMAgKHI9eWLL0chXAlfZ0~Z8dXaEv")
  spark.conf.set("fs.adl.oauth2.refresh.url",
    "https://login.microsoftonline.com/a8eec281-aaa3-4dae-ac9b-9a398b9215e7/oauth2/token")

  spark

}
