package interretis.utils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object SparkContextBuilder {

  def buildContext(appName: String): SparkContext = {

    val config = new SparkConf
    config setAppName appName

    new SparkContext(config)
  }

  def buildSqlContext(appName: String): SQLContext = {

    val context = buildContext(appName)

    new SQLContext(context)
  }
}
