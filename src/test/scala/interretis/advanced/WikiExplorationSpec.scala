package interretis.advanced

import org.scalatest.FlatSpec
import interretis.utils.SeparateSparkSQLContext
import org.scalatest.Matchers
import language.postfixOps

class WikiExplorationSpec extends SeparateSparkSQLContext with Matchers {

  "Application" should "count number of articles in Wiki" in { f =>

    // given
    val app = new WikiExploration(f.sqlSc)

    // when
    val count = app countArticles

    // then
    count shouldBe 39365
  }

  it should "retrieve top users in Wiki" in { f =>

    // given
    val app = new WikiExploration(f.sqlSc)

    // when
    val topUsers = app retrieveTopUsers

    // then
    topUsers should have size 10
  }

  it should "count number of articles that mention particular word" in { f =>

    // given
    val app = new WikiExploration(f.sqlSc)

    // when
    val count = app countArticlesWith "california"

    // then
    count shouldBe 1145
  }
}
