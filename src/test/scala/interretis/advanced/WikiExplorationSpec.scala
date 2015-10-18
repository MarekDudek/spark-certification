package interretis.advanced

import org.scalatest.FlatSpec
import interretis.utils.SeparateSparkSQLContext
import org.scalatest.Matchers

class WikiExplorationSpec extends SeparateSparkSQLContext with Matchers {

  "Application" should "count number of articles in Wiki" in { f =>

    // given
    WikiExploration loadWikiData f.sqlSc
    val app = new WikiExploration

    // when
    val count = app countArticles f.sqlSc

    // then
    count shouldBe 39365
  }
}
