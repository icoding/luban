package grimoire.ml.feature.transform

import grimoire.transform.Spell
import org.apache.spark.ml.linalg.{Vector, Vectors}
import play.api.libs.json.JsValue
import grimoire.Implicits.jstr2JsValue

/**
  * Created by caphael on 2017/3/23.
  */
class DoubleSeqToVectorSpell extends Spell[Seq[Double],Vector]{
  override def transformImpl(dat: Seq[Double]): Vector = {
    Vectors.dense(dat.toArray)
  }
}

object DoubleSeqToVectorSpell{
  def apply(json: JsValue="""{}"""): DoubleSeqToVectorSpell = new DoubleSeqToVectorSpell().parseJson(json)
}