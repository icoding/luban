package grimoire.ml.clustering.transform

import grimoire.configuration.param.HasInputCol
import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.clustering.LDAModel
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/22.
  */
class LDATransformSpell extends Spell[DataFrame & LDAModel,DataFrame]
  with HasInputCol{
  override def transformImpl(dat: DataFrame & LDAModel): DataFrame = {
    if (inputCol.isSet) dat._2.setFeaturesCol($(inputCol))
    dat._2.transform(dat._1)
  }
}

object LDATransformSpell{
  def apply(json: JsValue): LDATransformSpell =
    new LDATransformSpell().parseJson(json)
}