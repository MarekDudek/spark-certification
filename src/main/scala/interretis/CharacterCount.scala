package interretis

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import language.postfixOps

class CharacterCount {

  def countCharacter(verses: RDD[String], character: String): Long = {

    val mentions = verses filter (_ contains character)
    mentions count
  }
}

object CharacterCount {

  def main(args: Array[String]): Unit = {

    val actual = args.length
    val expected = 2

    if (actual != expected) {
      sys error (s"$expected arguments required and $actual given")
      sys exit 1
    }

    val book = args(0)
    val character = args(1)

    val config = new SparkConf
    config setAppName "Character count"
    val context = new SparkContext(config)

    val verses = context textFile book

    val app = new CharacterCount
    val count = app countCharacter (verses, character)

    println(s"Character $character is mentioned $count times in $book")
  }
}
