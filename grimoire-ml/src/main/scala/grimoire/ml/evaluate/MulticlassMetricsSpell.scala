package grimoire.ml.evaluate

import grimoire.ml.evaluate.result.MulticlassMetricsResult
import grimoire.transform.Spell
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, MultilabelMetrics}
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-19.
  */
class MulticlassMetricsSpell extends Spell[RDD[(Double, Double)],MulticlassMetricsResult]{

  override def setup(dat:RDD[(Double, Double)]): Boolean = {
    val mc = new MulticlassMetrics(dat)
    super.setup(dat)
  }
  override def transformImpl(dat: RDD[(Double, Double)] ): MulticlassMetricsResult = {
    val mc = new MulticlassMetrics(dat)

    MulticlassMetricsResult(
      mc.confusionMatrix,
      mc.accuracy,
      mc.weightedFMeasure,
      mc.weightedPrecision,
      mc.weightedRecall,
      mc.weightedTruePositiveRate,
      mc.weightedFalsePositiveRate
    )
  }
}

object MulticlassMetricsSpell{
  def apply(json: JsValue="""{}"""): MulticlassMetricsSpell =
    new MulticlassMetricsSpell().parseJson(json)
}
