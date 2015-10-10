package interretis.utils

import org.scalatest.Suite
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkContext

import language.postfixOps

trait SeparateSparkContext extends BeforeAndAfterAll {

  this: Suite =>

  var sc: SparkContext = null

  override def beforeAll: Unit = {
    sc = SparkTestContext.createTestContext()
    super.beforeAll
  }

  override def afterAll: Unit = {
    try
      super.afterAll
    finally
      sc stop
  }
}
