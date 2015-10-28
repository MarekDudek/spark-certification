package interretis.advanced.mlib

import interretis.utils.Resources
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import language.postfixOps

class MovieRatingsSuite extends FlatSpec with Matchers {

  val line = "0::780::1::1446056933"

  "My ratings" should "be possible to parse" in {

    // when
    val rating = MovieRatings ratingFromLine line

    // then
    rating.user shouldBe 0
    rating.product shouldBe 780
    rating.rating shouldBe 1
  }

  "My ratings" should "be read from file" in {

    // given
    val file = Resources.mainResources + "/personalRatings.txt"

    // when
    val ratings = MovieRatings loadMyRatings

    // then
    ratings should have size 8
  }
}
