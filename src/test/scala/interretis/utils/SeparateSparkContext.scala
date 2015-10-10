package interretis.utils

import org.apache.spark.SparkContext
import interretis.utils.TestSparkContext.createTestContext

import org.scalatest.fixture.FlatSpec
import org.scalatest.Outcome

import language.postfixOps

trait SeparateSparkContext extends FlatSpec {

  case class FixtureParam(sc: SparkContext)

  def withFixture(test: OneArgTest): Outcome = {

    val sc = createTestContext()

    try
      withFixture(test toNoArgTest FixtureParam(sc))
    finally
      sc stop
  }
}
