package grimoire.zeppelin.transform

import grimoire.ml.configuration.param.HasThreshold
import grimoire.ml.evaluate.result.BinaryClassificationMetricsResult
import grimoire.ml.linalg.LabeledMatrix
import grimoire.transform.Spell
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.zeppelin.Implicits._

/**
  * Created by sjc505 on 17-7-26.
  */
class ZepplinBinaryClassificationResultSpell extends Spell[BinaryClassificationMetricsResult,String] {


  override def transformImpl(dat: BinaryClassificationMetricsResult): String = {
    dat.toScript()
  }
}

object ZepplinBinaryClassificationResultSpell{
  def apply(json: JsValue ="""{}"""): ZepplinBinaryClassificationResultSpell =
    new ZepplinBinaryClassificationResultSpell().parseJson(json)
}
