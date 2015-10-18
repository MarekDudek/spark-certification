package interretis.advanced

import interretis.utils.SparkContextBuilder._
import interretis.utils.Resources._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

object WikiExploration {

  val dataDirectory = mainResources + "/wiki_parquet"

  def main(args: Array[String]): Unit = {

    val sqlContext = buildSqlContext(getClass.getSimpleName)
    loadWikiData(sqlContext)

    val app = new WikiExploration

    val count = app countArticles sqlContext
    println(s"There are $count articles in Wiki")
  }

  def loadWikiData(sqlContext: SQLContext): Unit = {

    val data = sqlContext.read.parquet(dataDirectory)
    data registerTempTable "wikiData"
  }
}

class WikiExploration {

  def countArticles(sqlContext: SQLContext): Long = {

    import sqlContext.implicits._

    val df = sqlContext.sql("SELECT count(*) FROM wikiData")
    df.collect.head.getLong(0)
  }
}
