package interretis.intro

import ClickRegister.{ uuidRegisterPairFromTokens, uuidClickPairFromTokens, splitByTab }
import interretis.utils.SparkContextBuilder.buildContext
import org.apache.spark.rdd.RDD
import java.util.Date
import java.text.SimpleDateFormat
import language.postfixOps

case class Register(date: Date, uuid: String, customerID: String, latitude: Float, longtitude: Float)

case class Click(date: Date, uuid: String, landingPage: Int)

class ClickRegister {

  def process(registerLines: RDD[String], clickLines: RDD[String]): RDD[(String, (Register, Click))] = {

    val registers = registerLines map splitByTab map uuidRegisterPairFromTokens
    val clicks = clickLines map splitByTab map uuidClickPairFromTokens

    val result = registers join clicks
    result
  }
}

object ClickRegister {

  private def newDateFormat =
    new SimpleDateFormat("yyyy-MM-dd")

  private def uuidRegisterPairFromTokens(tokens: Array[String]): (String, Register) = {
    val uuid = tokens(1)
    val register = registerFromTokens(tokens)
    (uuid, register)
  }

  private def registerFromTokens(tokens: Array[String]): Register =
    Register(newDateFormat parse tokens(0), tokens(1), tokens(2), tokens(3) toFloat, tokens(4) toFloat)

  private def uuidClickPairFromTokens(tokens: Array[String]): (String, Click) = {
    val uuid = tokens(1)
    val click = clickFromTokens(tokens)
    (uuid, click)
  }

  private def clickFromTokens(tokens: Array[String]): Click =
    Click(newDateFormat parse tokens(0), tokens(1), tokens(2).trim.toInt)

  def main(args: Array[String]): Unit = {

    val (registersInput, clicksInput, output) = processArguments(args)

    val sc = buildContext(appName = "Click Register")

    val registerLines = sc textFile registersInput
    val clickLines = sc textFile clicksInput

    val app = new ClickRegister

    val result = app process (registerLines, clickLines)
    result saveAsTextFile output
  }

  private def processArguments(args: Array[String]) = {

    val expected = 3
    val actual = args.length

    if (actual != expected) {
      sys error s"$expected arguments required and $actual given"
      sys exit 1
    }

    val registersInput = args(0)
    val clicksInput = args(1)
    val output = args(2)

    (registersInput, clicksInput, output)
  }

  private def splitByTab(arg: String) = arg split "\t"
}
