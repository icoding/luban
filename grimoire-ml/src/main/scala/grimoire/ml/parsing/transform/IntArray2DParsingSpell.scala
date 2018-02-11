package grimoire.ml.parsing.transform

import grimoire.Implicits._
import grimoire.ml.parsing.GenericParser
import grimoire.transform.Spell
import play.api.libs.json.JsValue
/**
  * Created by sjc505 on 17-6-30.
  */
class IntArray2DParsingSpell extends Spell[String,Array[Array[Int]]]{
  override def transformImpl(dat: String): Array[Array[Int]] = {
    IntArray2DParsingSpell.lineParser.parse(dat)
  }
}

object IntArray2DParsingSpell{
  def apply(json: JsValue="""{}"""): IntArray2DParsingSpell =
    new IntArray2DParsingSpell().parseJson(json)

  val lineParser = new GenericParser[Array[Array[Int]]] {
    val INT = "[0-9]+".r
    val LSQB = "["
    val RSQB = "]"

    def ARRAYD1 = LSQB ~> repsep(INT,",") <~ RSQB ^^ {
      case l =>
        l.map(_.toInt).toArray
    }
    def ARRAYD2 = ((LSQB ~> repsep(ARRAYD1,",") <~ RSQB) | repsep(ARRAYD1,",")) ^^ {
      case l =>
        l.toArray
    }

    override val lineParser: Parser[Array[Array[Int]]] = ARRAYD2
  }

}