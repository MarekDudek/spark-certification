package interretis

import interretis.utils.SparkContextBuilder.buildContext
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

    val (book, character) = processArguments(args)
    val sc = buildContext(appName = "Character Count")
    val verses = sc textFile book

    val app = new CharacterCount
    val count = app countCharacter (verses, character)

    println(s"Character $character is mentioned $count times in $book")
  }

  private def processArguments(args: Array[String]) = {

    val actual = args.length
    val expected = 2

    if (actual != expected) {
      sys error (s"$expected arguments required and $actual given")
      sys exit 1
    }

    val book = args(0)
    val character = args(1)

    (book, character)
  }
}
