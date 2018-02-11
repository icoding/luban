package grimoire.spark.transform.dataframe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.spark.configuration.param.HasSelectColNames
/**
  * Created by caphael on 2017/3/23.
  */
class DataFrameSelectSpell extends DataFrameSpell with HasSelectColNames{
  override def transformImpl(dat: DataFrame): DataFrame = {
    dat.select($(selectColNames).map(col(_)):_*)
  }
}

object DataFrameSelectSpell{
  def apply(json: JsValue="""{}"""): DataFrameSelectSpell = new DataFrameSelectSpell().parseJson(json)
}