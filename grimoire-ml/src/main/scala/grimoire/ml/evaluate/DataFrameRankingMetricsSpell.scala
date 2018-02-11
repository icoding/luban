package grimoire.ml.evaluate

import grimoire.Implicits._
import grimoire.ml.configuration.param._
import grimoire.ml.evaluate.result.RankingMetricsResult
import grimoire.transform.Spell
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json.JsValue
/**
  * Created by sjc505 on 17-7-19.
  */
class DataFrameRankingMetricsSpell extends Spell[DataFrame,RankingMetricsResult]
  with HasPrecisionAt with HasNdcgAt with HasLabelCol with HasPredictionCol{

  override def setup(dat:DataFrame): Boolean = {

    super.setup(dat)
  }
  override def transformImpl(dat: DataFrame ): RankingMetricsResult = {
    val dt=dat.select($(labelCol),$(predictionCol)).rdd.map{
      case Row(a:Array[Any],b:Array[Any]) =>
        (a,b)
    }
    val mc = new RankingMetrics(dt)
    RankingMetricsResult(
      mc.meanAveragePrecision,
      mc.precisionAt($(precisionAt)),
      mc.ndcgAt($(ndcgAt))
    )
  }
}

object DataFrameRankingMetricsSpell{
  def apply(json: JsValue="""{}"""): DataFrameRankingMetricsSpell =
    new DataFrameRankingMetricsSpell().parseJson(json)
}
