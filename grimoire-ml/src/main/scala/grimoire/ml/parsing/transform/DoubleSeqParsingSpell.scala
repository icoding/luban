package grimoire.ml.parsing.transform

import grimoire.ml.parsing.GenericParser
import grimoire.transform.Spell
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-12.
  */
class DoubleSeqParsingSpell extends Spell[String,Seq[Double]]{
  override def transformImpl(dat: String): Seq[Double] = {
    DoubleSeqParsingSpell.lineParser.parse(dat)
  }
}

object DoubleSeqParsingSpell {
  def apply(json: JsValue ="""{}"""): DoubleArray2DParsingSpell =
    new DoubleArray2DParsingSpell().parseJson(json)

  val lineParser = new GenericParser[Seq[Double]] {
    val DOUBLE = "[0.0-9.9]+".r

    def ARRAYD1 = repsep(DOUBLE, ",") ^^ {
      case l =>
        l.map(_.toDouble).toSeq
    }

    override val lineParser: Parser[Seq[Double]] = ARRAYD1
  }

}
