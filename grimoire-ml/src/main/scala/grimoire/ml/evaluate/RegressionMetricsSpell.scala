package grimoire.ml.evaluate

import grimoire.ml.evaluate.result.{MulticlassMetricsResult, RegressionMetricsResult}
import grimoire.transform.Spell
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, RegressionMetrics}
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-19.
  */
class RegressionMetricsSpell  extends Spell[RDD[(Double, Double)],RegressionMetricsResult]{

  override def setup(dat:RDD[(Double, Double)]): Boolean = {
    val mc = new RegressionMetrics(dat)
    super.setup(dat)
  }
  override def transformImpl(dat: RDD[(Double, Double)] ): RegressionMetricsResult = {
    val mc = new RegressionMetrics(dat)
    RegressionMetricsResult(
      mc.explainedVariance,
      mc.meanAbsoluteError,
      mc.meanSquaredError,
      mc.r2,
      mc.rootMeanSquaredError
    )
  }
}

object RegressionMetricsSpell{
  def apply(json: JsValue="""{}"""): RegressionMetricsSpell =
    new RegressionMetricsSpell().parseJson(json)
}

