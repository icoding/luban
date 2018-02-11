package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by caphael on 2017/6/23.
  */
class MinMaxScalerSpell extends DataFrameSpell with HasInputCol with HasOutputCol{
  val scaler = new MinMaxScaler()

  override def setup(dat: DataFrame): Boolean = {
    scaler.setInputCol($(inputCol)).setOutputCol($(outputCol))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    val scalerModel = scaler.fit(dat)
    scalerModel.transform(dat)
  }
}

object MinMaxScalerSpell{
  def apply(json: JsValue="""{}"""): MinMaxScalerSpell = new MinMaxScalerSpell().parseJson(json)
}
