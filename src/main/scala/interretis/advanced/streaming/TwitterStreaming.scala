package interretis.advanced.streaming

import Helper.configureTwitterCredentials
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter._
import language.postfixOps

object TwitterStreaming {

  def main(args: Array[String]): Unit = {

    val checkpointDir = "target/checkpoint/"

    val apiKey = "peOzkc52tjEa296IqinZSMj5F"
    val apiSecret = "9q2EZy32Gk2ZPVpioG7s0hE7Ahb51kMw5d41laX46S9veg3sfv"
    val accessToken = "1435349642-C1TYlJef7mCPpegj7QShKeuq8X717DU0bhOFmm4"
    val accessTokenSecret = "hWl4j9RugNSQ3HrjOpnh2KnN0kB8qPwa2XVpJIEMVIhaa"

    configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)

    val ssc = new StreamingContext(new SparkConf(), Seconds(1))

    val tweets = TwitterUtils.createStream(ssc, None)
    val statuses = tweets map (_ getText)
    statuses.print()

    ssc.checkpoint(checkpointDir)
    ssc.start()
    ssc.awaitTermination()
  }
}
