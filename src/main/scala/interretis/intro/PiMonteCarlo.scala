package interretis.intro

import interretis.utils.SparkContextBuilder.buildContext
import scala.math.random
import language.postfixOps
import org.apache.spark.rdd.RDD

class PiMonteCarlo {

  def compute(ordinals: RDD[Int], n: Int): Double = {

    val flags = ordinals map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }
    val sum = flags sum
    val pi = 4 * sum / n

    pi
  }
}

object PiMonteCarlo {

  def main(args: Array[String]): Unit = {

    val (n) = processArguments(args)
    val sc = buildContext(appName = "Pi Monte Carlo")
    val ordinals = sc parallelize (1 to n)

    val app = new PiMonteCarlo
    val pi = app compute (ordinals, n)

    println(s"Pi is roughly $pi")
  }

  private def processArguments(args: Array[String]) = {

    val expected = 1
    val actual = args.length

    if (actual != expected) {
      sys error s"$expected arguments required and $actual given"
      sys exit 1
    }

    val n = args(0).toInt

    (n)
  }
}
