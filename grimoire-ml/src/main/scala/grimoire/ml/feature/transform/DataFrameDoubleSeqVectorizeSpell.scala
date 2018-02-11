package grimoire.ml.feature.transform

import grimoire.transform.{HasSingleSpellLike, Spell}
import grimoire.Implicits.jstr2JsValue
import org.apache.spark.ml.linalg.Vector
import play.api.libs.json.JsValue
import grimoire.spark.transform.dataframe.DataFrameUDFSpell

/**
  * Created by caphael on 2017/6/23.
  */
class DataFrameDoubleSeqVectorizeSpell extends DataFrameUDFSpell with HasSingleSpellLike[Seq[Double],Vector]{


  override val single: Spell[Seq[Double], Vector] =  new SeqDoubleToVectorSpell()

  setUDF{
    (in:Seq[Double])=>
      single.transform(in)
  }
}

object DataFrameDoubleSeqVectorizeSpell{
  def apply(json: JsValue="""{}"""): DataFrameDoubleSeqVectorizeSpell = new DataFrameDoubleSeqVectorizeSpell().parseJson(json)
}
