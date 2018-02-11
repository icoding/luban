package grimoire.ml.classify.transform

import grimoire.configuration.param.HasOutputCol
import grimoire.ml.configuration.param.{HasFeatureCols, HasLabelCol}
import grimoire.Implicits._
import grimoire.dataset.&
import grimoire.spark.globalSpark
import grimoire.transform.{HasSingleSpellLike, Spell}
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/27.
  */
class DataFrameSVMPredictSpell
  extends Spell[DataFrame & SVMModel,DataFrame]
    with HasSingleSpellLike[Row & SVMModel,Row]
    with HasOutputCol
    with HasFeatureCols{

  override def transformImpl(dat: DataFrame & SVMModel): DataFrame = {
    single.setFeatureCols($(featureCols))
    val input & mod = dat
    val rdd = input.rdd.map{
      case r =>
        single.transform(r & mod)
    }
    val schema = input.schema.add($(outputCol),DoubleType)
    globalSpark.createDataFrame(rdd,schema)
  }

  override val single: SVMPredictSpell = new SVMPredictSpell().discardSchema()

  override def parseJson(json: JsValue): DataFrameSVMPredictSpell.this.type = {
    single.parseJson(json)
    super.parseJson(json)
  }
}

object DataFrameSVMPredictSpell{
  def apply(json: JsValue): DataFrameSVMPredictSpell = new DataFrameSVMPredictSpell().parseJson(json)
}