package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.ml.configuration.param.{HasNumFeatures, HasBinary}
import grimoire.spark.transform.dataframe.DataFrameSpell
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits.jstr2JsValue
import grizzled.binary

/**
  * Created by Arno on 2017/10/16.
  */
class DataFrameTFVectorizeSpell extends DataFrameSpell
  with HasInputCol with HasOutputCol with HasNumFeatures with HasBinary{
  val hashingTF = new HashingTF()

  override def setup(dat: DataFrame): Boolean = {
    hashingTF
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setNumFeatures($(numFeatures))
      .setBinary($(binary))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    hashingTF.transform(dat)
  }

}

object DataFrameTFVectorizeSpell{
  def apply(json:JsValue="""{}"""): DataFrameTFVectorizeSpell =
    new DataFrameTFVectorizeSpell().parseJson(json)
}