package interretis.advanced.mlib

import interretis.utils.SparkContextBuilder._
import interretis.utils.Resources
import org.apache.spark.mllib.recommendation.Rating
import scala.io.Source
import language.postfixOps

object MovieRatings {

  private val Ratings = Resources.mainResources + "/ratings.dat"
  private val Movies = Resources.mainResources + "/movies.dat"
  private val MyRatings = Resources.mainResources + "/personalRatings.txt"

  private val Separator = "::"

  def main(args: Array[String]): Unit = {

    val myRatings = loadRatings(MyRatings)
    val sc = buildContext("Movie recommender")
    val myRatingsRDD = sc parallelize myRatings

    val ratings = sc textFile Ratings map {
      line =>
        val fields = line split Separator
        (fields(3).toLong % 10, lineToRating(line))
    }

    val movies = sc textFile Movies map {
      line =>
        val fields = line split Separator
        (fields(0).toInt, fields(1))
    }
    val moviesMap = movies.collect.toMap

    println("ratings: " + ratings.count)
    println("movies: " + movies.count)

    sc stop
  }

  def loadRatings(file: String): Seq[Rating] = {
    val source = Source fromFile file
    val lines = source.getLines
    lines map lineToRating toSeq
  }

  def lineToRating(line: String): Rating = {
    val fields = line split Separator
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
  }
}
