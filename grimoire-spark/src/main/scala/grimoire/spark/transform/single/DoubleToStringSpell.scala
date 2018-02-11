package grimoire.spark.transform

import grimoire.transform.Spell
import play.api.libs.json.JsValue

/**
  * Created by sjc505 on 17-7-12.
  */
class StringToDoubleSpell extends Spell[String,Double]{
  override def transformImpl(dat: String): Double = {
    dat.toDouble
  }
}

object StringToDoubleSpell{
  def apply(json: JsValue): StringToDoubleSpell = new StringToDoubleSpell().parseJson(json)
}
