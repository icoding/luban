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
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json.JsValue

class DataFrameLBFGSPredictSpell extends Spell[DataFrame & LogisticRegressionModel,DataFrame]
  with HasSingleSpellLike[Row & LogisticRegressionModel,Row]
  with HasOutputCol
  with HasFeatureCols{

  override def transformImpl(dat: DataFrame & LogisticRegressionModel): DataFrame = {
    single.setFeatureCols($(featureCols))
    val input & mod = dat
    val rdd = input.rdd.map{
      case r =>
        single.transform(r & mod)
    }
    val schema = input.schema.add($(outputCol),DoubleType)
    globalSpark.createDataFrame(rdd,schema)
  }

  override val single: LBFGSPredictSpell = new LBFGSPredictSpell().discardSchema()

  override def parseJson(json: JsValue): DataFrameLBFGSPredictSpell.this.type = {
    single.parseJson(json)
    super.parseJson(json)
  }
}

object DataFrameLBFGSPredictSpell{
  def apply(json: JsValue): DataFrameLBFGSPredictSpell = new DataFrameLBFGSPredictSpell().parseJson(json)
}