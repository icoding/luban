package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.feature.{IndexToString, OneHotEncoder}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.HasDropLast
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by sjc505 on 17-6-21.
  */

class DataFrameOneHotEncoderSpell extends DataFrameSpell with HasInputCol with HasOutputCol
 with HasDropLast{
  val encoder = new OneHotEncoder()

  override protected def setup(dat: DataFrame): Boolean = {
    encoder
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setDropLast($(dropLast))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    encoder.transform(dat)
  }

}

object DataFrameOneHotEncoderSpell{
  def apply(json: JsValue="""{}"""): DataFrameOneHotEncoderSpell =
    new DataFrameOneHotEncoderSpell().parseJson(json)
}