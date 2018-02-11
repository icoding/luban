package grimoire.ml.classify.transform

/**
  * Created by jax on 17-6-29.
  */

import grimoire.configuration.param.HasOutputCol
import grimoire.ml.configuration.param.{HasFeatureCols, HasLabelCol}
import grimoire.Implicits._
import grimoire.dataset.&
import grimoire.spark.globalSpark
import grimoire.transform.{HasSingleSpellLike, Spell}
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json.JsValue

class DataFrameSGDPredictSpell
  extends Spell[DataFrame & LinearRegressionModel,DataFrame]
    with HasSingleSpellLike[Row & LinearRegressionModel,Row]
    with HasOutputCol
    with HasFeatureCols{

  override def transformImpl(dat: DataFrame & LinearRegressionModel): DataFrame = {
    single.setFeatureCols($(featureCols))
    val input & mod = dat
    val rdd = input.rdd.map{
      case r =>
        single.transform(r & mod)
    }
    val schema = input.schema.add($(outputCol),DoubleType)
    globalSpark.createDataFrame(rdd,schema)
  }

  override val single: SGDPredictSpell = new SGDPredictSpell().discardSchema()

  override def parseJson(json: JsValue): DataFrameSGDPredictSpell.this.type = {
    single.parseJson(json)
    super.parseJson(json)
  }
}

object DataFrameSGDPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameSGDPredictSpell = new DataFrameSGDPredictSpell().parseJson(json)
}