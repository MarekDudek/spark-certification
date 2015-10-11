package interretis.intro

import interretis.utils.SparkContextBuilder.buildContext
import JoinedWordcountsAssignment.containsSpark
import org.apache.spark.rdd.RDD

object JoinedWordcountsAssignment {

  val containsSpark = (line: String) => line contains "Spark"

  def main(args: Array[String]): Unit = {

    val (readmeFile, contributingFile, output) = processArguments(args)
    val sc = buildContext(appName = "Joined WordCounts")

    val readmeLines = sc textFile readmeFile
    val contributingLines = sc textFile contributingFile

    val app = new JoinedWordcountsAssignment
    val result = app process (readmeLines, contributingLines)

    result saveAsTextFile output
  }

  private def processArguments(args: Array[String]) = {

    val expected = 3
    val actual = args.length

    if (actual != expected) {
      sys error s"$expected arguments required and $actual given"
      sys exit 1
    }

    val readmeFile = args(0)
    val contributingFile = args(1)
    val output = args(2)

    (readmeFile, contributingFile, output)
  }
}

class JoinedWordcountsAssignment {

  def process(readmeLines: RDD[String], contributingLines: RDD[String]): RDD[(String, (Int, Int))] = {

    val wordCount = new WordCount

    val readmeSpark = readmeLines filter containsSpark
    val contributingSpark = contributingLines filter containsSpark

    val readmeWordCount = wordCount wordCount readmeSpark
    val contributingWordCount = wordCount wordCount contributingSpark

    val joined = readmeWordCount join contributingWordCount
    joined
  }
}
