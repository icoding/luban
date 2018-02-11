package grimoire.ml.evaluate

import grimoire.ml.configuration.param.{HasLabelCol, HasPredictionCol}
import grimoire.ml.evaluate.result.RegressionMetricsResult
import grimoire.transform.Spell
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json._
import grimoire.Implicits._

/**
  * Created by Arno on 17-11-15.
  */
class DataFrameRegressionMetricsSpell extends Spell[DataFrame,JsObject]
  with HasLabelCol with  HasPredictionCol{

  override def setup(dat:DataFrame): Boolean = {
    super.setup(dat)
  }
  override def transformImpl(dat:DataFrame ): JsObject = {
    val dt = dat.select($(labelCol),$(predictionCol)).rdd.map{
      case Row(a:Double,b:Double) =>
        (a,b)
    }
    val mc = new RegressionMetrics(dt)

    Json.obj(
      "explainedVariance" -> mc.explainedVariance,
      "meanAbsoluteError" -> mc.meanAbsoluteError,
      "meanSquaredError" -> mc.meanSquaredError,
      "r2" -> mc.r2,
      "rootMeanSquaredError" -> mc.rootMeanSquaredError
    )
  }
}

object DataFrameRegressionMetricsSpell{
  def apply(json: JsValue="""{}"""): DataFrameRegressionMetricsSpell =
    new DataFrameRegressionMetricsSpell().parseJson(json)
}


