package interretis.advanced.mlib

import interretis.utils.SparkContextBuilder._
import interretis.utils.Resources
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import scala.io.Source
import language.postfixOps
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object MovieRatings {

  private val Ratings = Resources.mainResources + "/ratings.dat"
  private val Movies = Resources.mainResources + "/movies.dat"
  private val MyRatings = Resources.mainResources + "/personalRatings.txt"

  private val Separator = "::"

  def main(args: Array[String]): Unit = {

    val sc = buildContext("Movie recommender")

    val myRatings = loadMyRatings()
    val myRatingsRDD = sc parallelize myRatings
    val timestampRatingRDD = loadRatings(sc)
    val movieRDD = loadMovies(sc)
    val moviesMap = movieRDD.collect.toMap
    reportStats(timestampRatingRDD)

    val (training, validation, test) = divide(timestampRatingRDD, myRatingsRDD)
    reportDivisionStats(training, validation, test)

    // train(timestampRatingRDD)

    sc stop
  }

  def loadMyRatings(): Seq[Rating] = {
    val source = Source fromFile MyRatings
    val lines = source.getLines
    lines map ratingFromLine toSeq
  }

  def loadRatings(sc: SparkContext): RDD[(Long, Rating)] = {
    sc textFile Ratings map {
      line =>
        val fields = line split Separator
        val timestamp = fields(3).toLong
        val rating = ratingFromLine(line)
        (timestamp % 10, rating)
    }
  }

  def loadMovies(sc: SparkContext): RDD[(Long, String)] = {
    sc textFile Movies map {
      line =>
        val fields = line split Separator
        val movieID = fields(0).toInt
        val title = fields(1)
        (movieID, title)
    }
  }

  def ratingFromLine(line: String): Rating = {
    val fields = line split Separator
    val userID = fields(0) toInt
    val movieID = fields(1) toInt
    val rating = fields(2) toDouble;
    Rating(userID, movieID, rating)
  }

  def reportStats(timestampRatingRDD: RDD[(Long, Rating)]): Unit = {
    val numRatings = timestampRatingRDD.count
    val numUsers = timestampRatingRDD.map { case (_, rating) => rating.user }.distinct.count
    val numMovies = timestampRatingRDD.map { case (_, rating) => rating.product }.distinct.count
    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")
  }

  def divide(timestampRatingRDD: RDD[(Long, Rating)], myRatingsRDD: RDD[Rating]): (RDD[Rating], RDD[Rating], RDD[Rating]) = {
    val numPartitions = 4
    val training = timestampRatingRDD.filter { case (timestamp, _) => timestamp < 6 }
      .values
      .union(myRatingsRDD)
      .repartition(numPartitions)
      .cache
    val validation = timestampRatingRDD.filter { case (timestamp, _) => timestamp >= 6 && timestamp < 8 }
      .values
      .repartition(numPartitions)
      .cache
    val test = timestampRatingRDD.filter { case (timestamp, _) => timestamp >= 8 }.values.cache
    (training, validation, test)
  }

  def reportDivisionStats(training: RDD[Rating], validation: RDD[Rating], test: RDD[Rating]): Unit = {
    val numTraining = training.count
    val numValidation = validation.count
    val numTest = test.count
    println(s"Training: $numTraining, validation: $numValidation, test: $numTest")
  }

  def train(timestampRatingRDD: RDD[(Long, Rating)]): Unit = {
    val ratings = timestampRatingRDD.map { case (_, rating) => rating }
    val rank = 1
    val iterations = 1
    val lambda = 0.5
    ALS.train(ratings, rank, iterations, lambda)
  }
}
