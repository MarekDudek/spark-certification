package interretis.intro.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import language.postfixOps

object NetworkWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      sys error "Usage NetworkWordCount <hostname> <port>"
      sys exit 1
    }

    val hostname = args(0)
    val port = args(1) toInt

    val sparkConf = new SparkConf() setAppName getClass.getSimpleName
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val lines = ssc socketTextStream (hostname, port, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines flatMap (_ split " ")
    val wordCounts = words map ((_, 1)) reduceByKey (_ + _)

    wordCounts.print
    ssc.start
    ssc awaitTermination
  }
}
