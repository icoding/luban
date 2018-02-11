package grimoire.ml.evaluate

import grimoire.Implicits._
import grimoire.ml.configuration.param.{HasNdcgAt, HasPrecisionAt}
import grimoire.ml.evaluate.result.RankingMetricsResult
import grimoire.transform.Spell
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

/**
  * Created by sjc505 on 17-7-6.
  */
class RankingMetricsSpell extends Spell[RDD[(Array[Double], Array[Double])],RankingMetricsResult]
  with HasPrecisionAt with HasNdcgAt{

  override def setup(dat:RDD[(Array[Double], Array[Double])]): Boolean = {
    val mc = new RankingMetrics(dat)
    super.setup(dat)
  }
  override def transformImpl(dat: RDD[(Array[Double], Array[Double])] ): RankingMetricsResult = {
    val mc = new RankingMetrics(dat)
    RankingMetricsResult(
      mc.meanAveragePrecision,
      mc.precisionAt($(precisionAt)),
      mc.ndcgAt($(ndcgAt))
    )
  }
}

object RankingMetricsSpell{
  def apply(json: JsValue="""{}"""): RankingMetricsSpell =
    new RankingMetricsSpell().parseJson(json)
}
