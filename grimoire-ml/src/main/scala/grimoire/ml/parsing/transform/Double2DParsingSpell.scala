package grimoire.ml.parsing.transform

import grimoire.ml.parsing.GenericParser
import grimoire.transform.Spell
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-19.
  */
class Double2DParsingSpell extends Spell[String,(Double,Double)]{
  override def transformImpl(dat: String): (Double,Double) = {
    Double2DParsingSpell.lineParser.parse(dat)
  }
}

object Double2DParsingSpell {
  def apply(json: JsValue ="""{}"""): DoubleArray2DParsingSpell =
    new DoubleArray2DParsingSpell().parseJson(json)

  val lineParser = new GenericParser[(Double,Double)] {
    val DOUBLE = "[0.0-9.9]+".r

    def ARRAYD1 =DOUBLE ~ "," ~ DOUBLE ^^ {
      case l~ "," ~b =>
        (l.toDouble,b.toDouble)
    }

    override val lineParser: Parser[(Double,Double)] = ARRAYD1
  }

}
