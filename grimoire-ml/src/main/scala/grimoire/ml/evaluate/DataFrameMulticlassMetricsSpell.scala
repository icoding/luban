package grimoire.ml.evaluate

import grimoire.ml.configuration.param.{HasLabelCol, HasPredictionCol}
import grimoire.ml.evaluate.result.MulticlassMetricsResult
import grimoire.transform.Spell
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json._
import grimoire.Implicits._

/**
  * Created by Arno on 17-12-26.
  */
class DataFrameMulticlassMetricsSpell extends Spell[DataFrame,JsObject]
  with HasLabelCol with  HasPredictionCol{

  override def setup(dat:DataFrame): Boolean = {
    super.setup(dat)
  }
  override def transformImpl(dat: DataFrame): JsObject = {
    val dt = dat.select($(predictionCol),$(labelCol)).rdd.map{
      case Row(a:Double,b:Double) =>
      (a,b)
    }
    val mc = new MulticlassMetrics(dt)
    Json.obj(
      "accuracy"->mc.accuracy.formatted("%.3f").toDouble,
      "weightedFMeasure" -> mc.weightedFMeasure.formatted("%.3f").toDouble,
      "weightedPrecision"-> mc.weightedPrecision.formatted("%.3f").toDouble,
      "weightedRecall" -> mc.weightedRecall.formatted("%.3f").toDouble,
      "weightedTruePositiveRate"-> mc.weightedTruePositiveRate.formatted("%.3f").toDouble,
      "weightedFalsePositiveRate"-> mc.weightedFalsePositiveRate.formatted("%.3f").toDouble
    )
  }
}

object DataFrameMulticlassMetricsSpell{
  def apply(json: JsValue="""{}"""): DataFrameMulticlassMetricsSpell =
    new DataFrameMulticlassMetricsSpell().parseJson(json)
}
