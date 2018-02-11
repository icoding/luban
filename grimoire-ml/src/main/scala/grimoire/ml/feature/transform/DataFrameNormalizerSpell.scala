package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.Implicits._
import grimoire.ml.configuration.param.HasP
import grimoire.spark.transform.dataframe.DataFrameSpell
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

/**
  * Created by sjc505 on 17-6-22.
  */
class DataFrameNormalizerSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasP{
  val normalizer = new Normalizer()

  override def setup(dat: DataFrame): Boolean = {
    normalizer
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setP($(p))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    normalizer.transform(dat)
  }
}

object DataFrameNormalizerSpell{
  def apply(json: JsValue="""{}"""): DataFrameNormalizerSpell =
    new DataFrameNormalizerSpell().parseJson(json)
}