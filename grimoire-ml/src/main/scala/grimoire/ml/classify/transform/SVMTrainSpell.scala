package grimoire.ml.classify.transform

import grimoire.configuration.param.HasRegParam
import grimoire.dataset.&
import grimoire.ml.Implicits._
import grimoire.ml.configuration.param._
import grimoire.ml.feature.transform.DataFrameMLLabeledPointSpell
import grimoire.transform.Spell
import grimoire.util.keeper.MapKeeper
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/27.
  */
class SVMTrainSpell
  extends Spell[DataFrame & MapKeeper[String,Long],SVMModel]
    with HasFeatureCols
    with HasLabelCol with HasStepSize with HasNumIter with HasRegParam with HasMiniBatchFrac {
  val labpntSpell = new DataFrameMLLabeledPointSpell

  override def parseJson(json: JsValue): SVMTrainSpell.this.type = {
    labpntSpell.parseJson(json)
    super.parseJson(json)
  }

  override def transformImpl(dat: DataFrame & MapKeeper[String, Long]): SVMModel = {
    val labpnt = labpntSpell.transform(dat)
    SVMWithSGD.train(labpnt,$(numIter),$(stepSize),$(regParam),$(miniBatchFrac))
  }
}

object SVMTrainSpell{
  def apply(json: JsValue): SVMTrainSpell = new SVMTrainSpell().parseJson(json)
}