package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.spark.transform.dataframe.DataFrameSpell
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.sql.DataFrame

/**
  * Created by Arno on 17-10-19.
  */
class DataFrameMinMaxScalerSpell extends DataFrameSpell with HasInputCol with HasOutputCol{
  val scaler = new MinMaxScaler()

  override def setup(dat: DataFrame): Boolean = {

    scaler
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    scaler.fit(dat).transform(dat)
  }
}

object DataFrameMinMaxScalerSpell{
  def apply(json: JsValue="""{}"""): DataFrameMinMaxScalerSpell =
    new DataFrameMinMaxScalerSpell().parseJson(json)
}
