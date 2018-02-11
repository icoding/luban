package grimoire.ml.classify.transform


import grimoire.ml.configuration.param.{HasLabelCol, HasMetricName, HasPredictionCol}
import grimoire.transform.Spell
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

/**
  * Created by sjc505 on 17-6-28.
  */
class DataFrameMulticlassClassificationEvaluatorSpell
  extends Spell[DataFrame,Double]
  with HasLabelCol with HasMetricName with HasPredictionCol {

  val mc = new MulticlassClassificationEvaluator()

  override def setup(dat: DataFrame): Boolean = {
    mc
      .setLabelCol($(labelCol))
      .setPredictionCol($(predictionCol))
      .setMetricName($(metricName))

    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame ): Double = {
    mc.evaluate(dat)
  }
}

object DataFrameMulticlassClassificationEvaluatorSpell{
  def apply(json: JsValue="""{}"""): DataFrameMulticlassClassificationEvaluatorSpell =
    new DataFrameMulticlassClassificationEvaluatorSpell().parseJson(json)
}