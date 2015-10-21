package interretis.advanced.mlib

import interretis.utils.SparkContextBuilder._
import org.apache.spark.mllib.recommendation.Rating
import interretis.utils.Resources

object MovieRatings {

  def main(args: Array[String]): Unit = {

    val sc = buildContext("Movie recommender")

    val ratings = sc textFile (Resources.mainResources + "/ratings.dat") map {
      line =>
        val fields = line split "::"
        (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    val movies = sc textFile (Resources.mainResources + "/movies.dat") map {
      line =>
        val fields = line split "::"
        (fields(0).toInt, fields(1))
    }

    println("ratings: " + ratings.count)
    println("movies: " + movies.count)
  }
}
