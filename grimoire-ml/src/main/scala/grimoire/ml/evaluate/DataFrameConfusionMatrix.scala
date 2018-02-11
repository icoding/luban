package grimoire.ml.evaluate
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json._
import grimoire.Implicits._

/**
  * Created by Arno on 17-12-29.
  */

class DataFrameConfusionMatrix extends Spell[DataFrame,JsValue]
  with HasIndexedlabelCol with  HasOriginlabelCol with HasPredictionCol{

  override def setup(dat:DataFrame): Boolean = {
    super.setup(dat)
  }
  override def transformImpl(dat: DataFrame): JsValue = {
    val dt = dat.select($(predictionCol),$(indexedlabelCol)).rdd.map{
      case Row(a:Double,b:Double) =>
        (a,b)
    }
    val mc = new MulticlassMetrics(dt)


    if(originlabelCol.getOrDefault() != null) {
      val map2Indexlabel = dat.select($(indexedlabelCol),$(originlabelCol)).
        distinct().rdd.collect.
        map(_.toSeq.toArray).map(x => (x(0).toString, x(1).toString)).toMap

      val matrixInside = mc.confusionMatrix.toArray.sliding(mc.labels.length, mc.labels.length).
        map(mc.labels.map(_.toString).map(map2Indexlabel(_)) zip _ toMap).toList
      Json.toJson(mc.labels.map(_.toString).map(map2Indexlabel(_)) zip matrixInside toMap)
    } else {
      val matrixInside = mc.confusionMatrix.toArray.sliding(mc.labels.length, mc.labels.length).
        map(mc.labels.map(_.toString) zip _ toMap).toList
      Json.toJson(mc.labels.map(_.toString) zip matrixInside toMap)

    }
  }
}

object DataFrameConfusionMatrix{
  def apply(json: JsValue="""{}"""): DataFrameConfusionMatrix =
    new DataFrameConfusionMatrix().parseJson(json)
}
