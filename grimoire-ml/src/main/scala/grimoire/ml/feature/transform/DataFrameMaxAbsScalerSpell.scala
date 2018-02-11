package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.feature.MaxAbsScaler
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by sjc505 on 17-6-23.
  */
class DataFrameMaxAbsScalerSpell extends  DataFrameSpell with HasInputCol
  with HasOutputCol{
  val scaler = new MaxAbsScaler()

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

object DataFrameMaxAbsScalerSpell{
  def apply(json: JsValue="""{}"""): DataFrameMaxAbsScalerSpell =
    new DataFrameMaxAbsScalerSpell().parseJson(json)
}
