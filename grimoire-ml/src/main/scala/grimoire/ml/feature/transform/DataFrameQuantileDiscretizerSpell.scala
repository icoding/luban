package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.{HasHandleInvalid, HasNumBuckets, HasRelativeError}
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by Arno on 17-10-19.
  */
class DataFrameQuantileDiscretizerSpell extends DataFrameSpell
  with HasInputCol with HasOutputCol with HasNumBuckets
  with HasHandleInvalid with HasRelativeError{
  val discretizer = new QuantileDiscretizer()

  override def setup(dat: DataFrame): Boolean = {

    discretizer
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setNumBuckets($(numBuckets))
      .setHandleInvalid($(handleInvalid))
      .setRelativeError($(relativeError))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    discretizer.fit(dat).transform(dat)
  }

}


object DataFrameQuantileDiscretizerSpell{
  def apply(json: JsValue="""{}"""): DataFrameQuantileDiscretizerSpell =
    new DataFrameQuantileDiscretizerSpell().parseJson(json)
}