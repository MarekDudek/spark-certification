package interretis.advanced

import interretis.utils.SparkContextBuilder._
import interretis.utils.Resources._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import language.postfixOps

object WikiExploration {

  private val dataDirectory = mainResources + "/wiki_parquet"
  private val table = "wikiData"

  def main(args: Array[String]): Unit = {

    val sqlContext = buildSqlContext(getClass.getSimpleName)

    val app = new WikiExploration(sqlContext)

    val count = app.countArticles
    println(s"There are $count articles in Wiki")

    val topUsers = app.retrieveTopUsers
    topUsers foreach (println)

    val californiaArticles = app countArticlesWith "california"
    println(s"There are $californiaArticles that mention California")
  }

  private def loadWikiData(sqlContext: SQLContext): Unit = {
    val data = sqlContext.read.parquet(dataDirectory)
    data registerTempTable table
  }
}

case class TopUser(name: String, count: Long)

class WikiExploration(sqlContext: SQLContext) {

  import sqlContext._
  import sqlContext.implicits._
  import WikiExploration.{ loadWikiData, table }

  loadWikiData(sqlContext)

  def countArticles: Long = {
    val df = sql(s"SELECT count(*) FROM $table")
    df.collect.head.getLong(0)
  }

  def retrieveTopUsers: Array[TopUser] = {
    val df = sql(s"SELECT username, count(*) AS cnt FROM $table WHERE username <> '' GROUP BY username ORDER BY cnt DESC LIMIT 10")
    val topUsers = df map {
      case Row(username: String, count: Long) => TopUser(username, count)
    }
    topUsers collect
  }

  def countArticlesWith(word: String): Long = {
    val df = sql(s"SELECT count(*) FROM $table WHERE text LIKE '%$word%'")
    df.collect.head.getLong(0)
  }
}
