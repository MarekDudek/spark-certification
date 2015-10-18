package interretis.utils

import interretis.utils.TestSparkContext.{ createTestContext, createTestSqlContext }
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.Outcome
import org.scalatest.fixture.FlatSpec
import language.postfixOps

trait SeparateSparkSQLContext extends FlatSpec {

  case class FixtureParam(sc: SparkContext, sqlSc: SQLContext)

  def withFixture(test: OneArgTest): Outcome = {

    val context = createTestContext()
    val sqlContext = createTestSqlContext(context)

    try
      withFixture(test toNoArgTest FixtureParam(context, sqlContext))
    finally
      context stop
  }
}
