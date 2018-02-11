package grimoire.ml.parsing.transform

import grimoire.transform.Spell
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.parsing.GenericParser

/**
  * Created by sjc505 on 17-7-6.
  */
class DoubleArray2DParsingSpell extends Spell[String,(Array[Double],Array[Double])]{
  override def transformImpl(dat: String): (Array[Double],Array[Double]) = {
    DoubleArray2DParsingSpell.lineParser.parse(dat)
  }
}

object DoubleArray2DParsingSpell {
  def apply(json: JsValue ="""{}"""): DoubleArray2DParsingSpell =
    new DoubleArray2DParsingSpell().parseJson(json)

  val lineParser = new GenericParser[(Array[Double], Array[Double])] {
    val DOUBLE = "[0.0-9.9]+".r
    val LSQB = "["
    val RSQB = "]"

    def ARRAYD1 = LSQB ~> repsep(DOUBLE, ",") <~ RSQB ^^ {
      case l =>
        l.map(_.toDouble).toArray
    }

    def ARRAYD2 = LSQB ~> repsep(DOUBLE, ",") <~ RSQB ^^ {
      case l =>
        l.map(_.toDouble).toArray
    }

    def ARRAYD3 =  ARRAYD1~ "," ~ ARRAYD2 ^^ {
      case l ~ _ ~ b  =>
        (l,b)
    }

    override val lineParser: Parser[(Array[Double], Array[Double])] = ARRAYD3
  }

}