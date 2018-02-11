package grimoire.zeppelin.transform

import grimoire.ml.linalg.LabeledMatrix
import grimoire.transform.Spell
import grimoire.zeppelin.Implicits._
import play.api.libs.json.JsValue
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.configuration.param.HasThreshold


/**
  * Created by caphael on 2017/7/19.
  */
class ZeppelinLabeledMatrixSpell extends Spell[LabeledMatrix,String] with HasThreshold{
  threshold.optional().default(0.8)

  override def transformImpl(dat: LabeledMatrix): String = {
    dat.setThreshold($(threshold)).toScript()
  }
}

object ZeppelinLabeledMatrixSpell{
  def apply(json: JsValue ="""{}"""): ZeppelinLabeledMatrixSpell = new ZeppelinLabeledMatrixSpell().parseJson(json)
}