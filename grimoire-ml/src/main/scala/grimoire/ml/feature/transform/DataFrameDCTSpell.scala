package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.ml.configuration.param.HasInverse
import org.apache.spark.ml.feature.DCT
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by sjc505 on 17-6-21.
  */
class DataFrameDCTSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasInverse{
  val dct = new DCT()
  inverse.default(false)

  override def setup(dat: DataFrame): Boolean ={
    dct
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setInverse($(inverse))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    dct.transform(dat)
  }

}

object DataFrameDCTSpell{
  def apply(json: JsValue="""{}"""): DataFrameDCTSpell =
    new DataFrameDCTSpell().parseJson(json)
}
