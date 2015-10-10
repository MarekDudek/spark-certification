package interretis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import language.postfixOps

object SimpleApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf() setAppName ("Simple Application")
    val sc = new SparkContext(conf)

    val book = "src/main/resources/alice-in-wonderland.txt"

    val verses = sc textFile book cache
    val alices = verses filter (_.contains("Alice"))

    val count = alices.count
    println("There were " + count + " lines that Alice is in")
  }
}
