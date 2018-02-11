package grimoire.ml.feature.transform

import grimoire.transform.Spell
import org.apache.spark.ml.linalg.{Vector, Vectors}
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/23.
  */
class SeqDoubleToVectorSpell extends Spell[Seq[Double],Vector]{
  override def transformImpl(dat: Seq[Double]): Vector = {
    Vectors.dense(dat.toArray)
  }
}

object SeqDoubleToVectorSpell{
  def apply(json: JsValue): SeqDoubleToVectorSpell = new SeqDoubleToVectorSpell().parseJson(json)
}