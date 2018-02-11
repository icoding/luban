package grimoire.ml.target

import grimoire.target.DualTarget
import grimoire.transform.Spell
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.mllib.util.Saveable
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/2/13.
  */
class ModelTarget extends DualTarget[MLWritable,Saveable]{
  override val spell: Spell[MLWritable, Unit] = new MLWritableTarget
  override val alternative: Spell[Saveable, Unit] = new SaveableTarget
}

object ModelTarget{
  def apply(json:JsValue): ModelTarget = new ModelTarget().parseJson(json)
}