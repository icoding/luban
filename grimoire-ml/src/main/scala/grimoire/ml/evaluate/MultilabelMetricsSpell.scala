package grimoire.ml.evaluate

import grimoire.transform.Spell
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.evaluate.result.MultilabelMetricsResult
import org.apache.spark.mllib.evaluation.{MultilabelMetrics}
import org.apache.spark.rdd.RDD

/**
  * Created by sjc505 on 17-7-5.
  */
class MultilabelMetricsSpell extends Spell[RDD[(Array[Double], Array[Double])],MultilabelMetricsResult]{

  override def setup(dat:RDD[(Array[Double], Array[Double])]): Boolean = {
    val mc = new MultilabelMetrics(dat)
    super.setup(dat)
  }
  override def transformImpl(dat: RDD[(Array[Double], Array[Double])] ): MultilabelMetricsResult = {
    val mc = new MultilabelMetrics(dat)
    MultilabelMetricsResult (
      mc.accuracy,
      mc.subsetAccuracy,
      mc.f1Measure,
      mc.precision,
      mc.recall,
      mc.hammingLoss,
      mc.labels,
      mc.microF1Measure,
      mc.microPrecision,
      mc.microRecall
    )
  }
}

object MultilabelMetricsSpell{
  def apply(json: JsValue="""{}"""): MultilabelMetricsSpell =
    new MultilabelMetricsSpell().parseJson(json)
}