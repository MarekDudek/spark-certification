package interretis.intro.sql

import interretis.utils.Resources
import interretis.utils.SeparateSparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.Matchers
import language.postfixOps
import org.apache.spark.sql.hive.HiveContext

case class Person(name: String, age: Int)

class PeopleWithAgesSuite extends SeparateSparkContext with Matchers {

  "Spark" should "operate on data with SQL" in { f =>

    // given
    val sqlContext = new SQLContext(f.sc)
    import sqlContext.implicits._

    // when
    val peopleLines = f.sc textFile (Resources.mainResources + "/people.txt")
    val people = peopleLines map (_ split ",") map (p => Person(p(0), p(1).trim.toInt)) toDF

    people.registerTempTable("people")
    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    // then
    teenagers.count shouldBe 1
  }

  ignore should "operate on Hive" in { f =>

    val hiveContext = new HiveContext(f.sc)
    import hiveContext._
    import hiveContext.implicits._

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    sql("src/main/resources/kv1.txt' INTO TABLE src")
    sql("FROM src SELECT key, value").collect foreach(println)
  }
}
