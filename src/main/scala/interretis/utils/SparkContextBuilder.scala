package interretis.utils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkContextBuilder {

  def buildContext(appName: String): SparkContext = {

    val config = new SparkConf
    config setAppName appName

    val context = new SparkContext(config)
    context
  }
}
