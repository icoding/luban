package grimoire.ml.evaluate

import grimoire.ml.configuration.param.{HasLabelCol, HasNumBins, HasPredictionCol}
import grimoire.ml.evaluate.result.BinaryClassificationMetricsResult
import org.apache.spark.ml.linalg.Vector
import grimoire.transform.Spell
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json._
import grimoire.Implicits._

/**
  * Created by Arno on 17-7-14.
  */
class DataFrameBinaryClassificationMetricsSpell  extends Spell[DataFrame,JsObject]
  with HasLabelCol with  HasPredictionCol {

  override def setup(dat:DataFrame): Boolean = {
    super.setup(dat)
  }
  override def transformImpl(dat: DataFrame): JsObject = {
    val dt = dat.select($(predictionCol),$(labelCol)).rdd.map{
      case Row(a:Double,b:Double) =>
        (a,b)
      case Row(a:Vector,b:Double) =>
        (Row(a:Vector,b:Double).getAs[Vector](0)(1), Row(a:Vector,b:Double).getAs[Double](1))
    }
    val bcm = new BinaryClassificationMetrics(dt)

    Json.obj(
      "precisionByThreshold"->bcm.precisionByThreshold.map(_._2.formatted("%.3f").toDouble).collect,
      "recallByThreshold" -> bcm.recallByThreshold.map(_._2.formatted("%.3f").toDouble).collect,
      "roc"->bcm.roc.map(x=>List(x._1.formatted("%.3f").toDouble,x._2.formatted("%.3f").toDouble)).collect,
      "pr" -> bcm.pr.map(x=>List(x._1.formatted("%.3f").toDouble,x._2.formatted("%.3f").toDouble)).collect,
      "fMeasureByThreshold"->bcm.fMeasureByThreshold.map(_._2.formatted("%.3f").toDouble).collect,
      "areaUnderPR"->bcm.areaUnderPR.formatted("%.3f").toDouble,
      "areaUnderROC"->bcm.areaUnderROC.formatted("%.3f").toDouble,
      "thresholds"->bcm.thresholds.map(_.formatted("%.3f").toDouble).collect
    )
  }
}

object DataFrameBinaryClassificationMetricsSpell{
  def apply(json: JsValue="""{}"""): DataFrameBinaryClassificationMetricsSpell =
    new DataFrameBinaryClassificationMetricsSpell().parseJson(json)
}

