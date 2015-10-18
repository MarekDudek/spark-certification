package interretis.advanced

import interretis.utils.SparkContextBuilder._
import interretis.utils.Resources._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

object WikiExploration {

  private val dataDirectory = mainResources + "/wiki_parquet"
  private val table = "wikiData"

  def main(args: Array[String]): Unit = {

    val sqlContext = buildSqlContext(getClass.getSimpleName)

    val app = new WikiExploration(sqlContext)

    val count = app.countArticles
    println(s"There are $count articles in Wiki")
  }

  private def loadWikiData(sqlContext: SQLContext): Unit = {
    val data = sqlContext.read.parquet(dataDirectory)
    data registerTempTable table
  }
}

class WikiExploration(sqlContext: SQLContext) {

  import sqlContext.implicits._
  import WikiExploration.{ loadWikiData, table }

  loadWikiData(sqlContext)

  def countArticles: Long = {
    val df = sqlContext.sql(s"SELECT count(*) FROM $table")
    df.collect.head.getLong(0)
  }
}
