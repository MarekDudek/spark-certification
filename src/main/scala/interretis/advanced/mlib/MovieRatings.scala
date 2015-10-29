package interretis.advanced.mlib

import interretis.utils.SparkContextBuilder._
import interretis.utils.Resources
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import scala.io.Source
import language.postfixOps
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

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

    val bestModel = train(training, validation, test)

    val myRatedMovieIDs = myRatings.map(_.product).toSet
    val candidates = sc.parallelize(moviesMap.keys.filter(!myRatedMovieIDs.contains(_)).toSeq)
    val recommendations = bestModel.predict(candidates.map((0, _))).collect.sortBy(-_.rating).take(50)

    val a = recommendations.zipWithIndex.foreach {
      case (rating, index) =>
        println("%2d : %s".format(index, moviesMap(rating.product)))
    }

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

  def loadMovies(sc: SparkContext): RDD[(Int, String)] = {
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

  private val ranks = List(8, 12)
  private val lambdas = List(1.0, 10.0)
  private val numIters = List(10, 20)

  def train(training: RDD[Rating], validation: RDD[Rating], test: RDD[Rating]): MatrixFactorizationModel = {

    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1

    for {
      rank <- ranks;
      lambda <- lambdas;
      numIter <- numIters
    } {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation)
      println(s"RMSE (validation) = $validationRmse for the model trained with rank = $rank, lambda = $lambda, and numIter = $numIter.")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }

    val testRmse = computeRmse(bestModel.get, test)
    println(s"The best model was trained with rank = $bestRank and lambda = $bestLambda, and numIter = $bestNumIter, and its RMSE on test set is $testRmse.")
    bestModel.get
  }

  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    val count = data.count
    val predictions: RDD[Rating] = model.predict(data.map(rating => (rating.user, rating.product)))
    val predictionsAndRatings = predictions.map(rating => ((rating.user, rating.product), rating.rating))
      .join(data.map(rating => ((rating.user, rating.product), rating.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / count)
  }

  def recommedForMe(): Unit = {

  }
}
