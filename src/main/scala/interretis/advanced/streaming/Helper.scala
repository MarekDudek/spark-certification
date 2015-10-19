package interretis.advanced.streaming

import scala.collection.mutable.HashMap
import sys.process.stringSeqToProcess

object Helper {

  def configureTwitterCredentials(apiKey: String, apiSecret: String, accessToken: String, accessTokenSecret: String): Unit = {

    val configs = new HashMap[String, String] ++= Seq(
      "apiKey" -> apiKey,
      "apiSecret" -> apiSecret,
      "accessToken" -> accessToken,
      "accessTokenSecret" -> accessTokenSecret)

    println("Configuring Twitter OAuth")

    configs.foreach {
      case (key, value) =>
        if (value.trim.isEmpty) {
          throw new Exception("Error setting authentication - value for " + key + " not set")
        }
        val fullKey = "twitter4j.oauth." + key.replace("api", "consumer")
        System.setProperty(fullKey, value.trim)
        println("\tProperty " + fullKey + " set as [" + value.trim + "]")
    }
    println()
  }
}
