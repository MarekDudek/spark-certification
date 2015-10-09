package interretis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SimpleApp {

  val book = "src/main/resources/alice-in-wonderland.txt"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val verses = sc.textFile(book, 8).cache()
    val alices = verses.filter(line => line.contains("Alice"))

    val count = alices.count
    println("There were " + count + " lines that Alice is in")
  }
}