package grimoire.ml.classify.transform

/**
  * Created by jax on 17-6-29.
  */
import grimoire.configuration.param
import grimoire.dataset.&
import grimoire.ml.Implicits._
import grimoire.Implicits._
import grimoire.ml.configuration.param._
import grimoire.ml.feature.transform.DataFrameMLLabeledPointSpell
import grimoire.transform.Spell
import grimoire.util.keeper.MapKeeper
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD


class LinearRegressionWithSGDTrainSpell
  extends Spell[DataFrame & MapKeeper[String,Long],LinearRegressionModel]
    with HasFeatureCols with HasLabelCol with HasStepSize with HasNumIterations {
  val labpntSpell = new DataFrameMLLabeledPointSpell

  override def parseJson(json: JsValue): LinearRegressionWithSGDTrainSpell.this.type = {
    labpntSpell.parseJson(json)
    super.parseJson(json)
  }

  override def transformImpl(dat: DataFrame & MapKeeper[String, Long]): LinearRegressionModel = {
    val labpnt = labpntSpell.setLabelCol($(labelCol)).setFeatureCols($(featureCols)).transform(dat)
    LinearRegressionWithSGD.train(labpnt,$(numIterations),$(stepSize))
  }
}

object LinearRegressionWithSGDTrainSpell{
  def apply(json: JsValue): LinearRegressionWithSGDTrainSpell = new LinearRegressionWithSGDTrainSpell().parseJson(json)
}