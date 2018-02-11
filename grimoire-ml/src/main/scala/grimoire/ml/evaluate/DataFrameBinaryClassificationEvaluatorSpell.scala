package grimoire.ml.evaluate

import grimoire.ml.configuration.param.{HasLabelCol, HasMetricName, HasRawPredictionCol}
import grimoire.transform.Spell
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-5.
  */
class DataFrameBinaryClassificationEvaluatorSpell extends Spell[DataFrame,Double]
  with HasLabelCol with HasMetricName with HasRawPredictionCol {

  val bc = new BinaryClassificationEvaluator()

  override def setup(dat: DataFrame): Boolean = {
    bc
      .setLabelCol($(labelCol))
      .setRawPredictionCol($(rawPredictionCol))
      .setMetricName($(metricName))

    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame ): Double = {
    bc.evaluate(dat)
  }
}

object DataFrameBinaryClassificationEvaluatorSpell{
  def apply(json: JsValue="""{}"""): DataFrameBinaryClassificationEvaluatorSpell =
    new DataFrameBinaryClassificationEvaluatorSpell().parseJson(json)
}