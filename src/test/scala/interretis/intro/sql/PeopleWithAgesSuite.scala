package interretis.intro.sql

import interretis.utils.Resources
import interretis.utils.SeparateSparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.Matchers
import language.postfixOps
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row
import scala.collection.mutable.WrappedArray

case class Person(name: String, age: Int)

class PeopleWithAgesSuite extends SeparateSparkContext with Matchers {

  "Spark" should "operate on data with SQL" in { f =>

    // given
    val sqlContext = new SQLContext(f.sc)
    import sqlContext.implicits._

    // when
    val peopleLines = f.sc textFile (Resources.mainResources + "/people.txt")
    val people = peopleLines map (_ split ",") map (p => Person(p(0), p(1).trim.toInt)) toDF

    people registerTempTable "people"
    val teenagers = sqlContext sql "SELECT name FROM people WHERE age >= 13 AND age <= 19"

    // then
    teenagers.count shouldBe 1
  }

  ignore should "operate on Hive" in { f =>

    // given
    val hiveContext = new HiveContext(f.sc)
    import hiveContext._
    import hiveContext.implicits._

    // when
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    sql("src/main/resources/kv1.txt' INTO TABLE src")
    sql("FROM src SELECT key, value").collect foreach (println)
  }

  "Spark" should "work with Parquet files" in { f =>

    // given
    val sqlContext = new SQLContext(f.sc)
    import sqlContext._
    import sqlContext.implicits._

    // when
    val parquetFile = read parquet (Resources.mainResources + "/users.parquet")
    parquetFile registerTempTable "users"

    // then
    parquetFile.count shouldBe 2
    parquetFile.columns shouldBe Array("name", "favorite_color", "favorite_numbers")

    // when
    val red = sql("SELECT name FROM users WHERE favorite_color = 'red'")
    val Row(name: String) = red.first

    // then
    red.count shouldBe 1
    name shouldBe "Ben"

    // when
    val rows = sqlContext sql "SELECT name, favorite_color, favorite_numbers FROM users WHERE name = 'Ben'"
    val objects = rows map {
      case Row(name: String, favorite_color: String, favorite_numbers: Seq[String] @unchecked) =>
        (name, favorite_color, favorite_numbers.toList)
    }

    // then
    objects.collect should have length 1
  }
}
