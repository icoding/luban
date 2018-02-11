package grimoire.ml.evaluate

import grimoire.transform.Spell
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.evaluate.result.BinaryClassificationMetricsResult
import org.apache.spark.sql.DataFrame

/**
  * Created by caphael on 2017/7/14.
  */
class BinaryClassificationMetricsSpell extends Spell[RDD[(Double,Double)],BinaryClassificationMetricsResult]{

  override def setup(dat:RDD[(Double,Double)]): Boolean = {
    super.setup(dat)
  }
  override def transformImpl(dat: RDD[(Double, Double)]): BinaryClassificationMetricsResult = {

    val bcm = new BinaryClassificationMetrics(dat)

    BinaryClassificationMetricsResult(
        bcm.fMeasureByThreshold,
        bcm.precisionByThreshold,
        bcm.recallByThreshold,
        bcm.roc,
        bcm.areaUnderPR,
        bcm.areaUnderROC,
        bcm.thresholds
    )
  }
}
object BinaryClassificationMetricsSpell{
  def apply(json: JsValue="""{}"""): BinaryClassificationMetricsSpell =
    new BinaryClassificationMetricsSpell().parseJson(json)
}