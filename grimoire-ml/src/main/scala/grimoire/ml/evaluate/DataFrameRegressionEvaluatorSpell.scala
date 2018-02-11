package grimoire.ml.evaluate

/**
  * Created by jax on 17-7-3.
  */

import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.DataFrame

class DataFrameRegressionEvaluatorSpell
  extends Spell[DataFrame,Double]
    with HasLabelCol with HasMetricName with HasPredictionCol
{

  val re = new RegressionEvaluator()

  override def setup(dat: DataFrame): Boolean = {
    re
      .setLabelCol($(labelCol))
      .setPredictionCol($(predictionCol))
      .setMetricName($(metricName))

    super.setup ( dat )
  }

  override def transformImpl(dat: DataFrame): Double = {
    re.evaluate(dat)
  }
}

object DataFrameRegressionEvaluatorSpell{
  def apply(json: JsValue="""{}"""): DataFrameRegressionEvaluatorSpell =
    new DataFrameRegressionEvaluatorSpell ().parseJson(json)
}