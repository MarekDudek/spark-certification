package interretis.utils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TestSparkContext {

  def createTestContext(appName: String = "Test application", master: String = "local"): SparkContext = {

    val config = new SparkConf

    config setAppName appName
    config setMaster master

    new SparkContext(config)
  }
}
