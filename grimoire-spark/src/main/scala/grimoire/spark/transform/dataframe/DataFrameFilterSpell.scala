package grimoire.spark.transform.dataframe

import grimoire.spark.configuration.param.HasFilterExpr
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits.jstr2JsValue

/**
  * Created by caphael on 2017/3/27.
  */
class DataFrameFilterSpell extends DataFrameSpell with HasFilterExpr{
  override def transformImpl(dat: DataFrame): DataFrame = {
    dat.filter($(filterExpr))
  }
}

object DataFrameFilterSpell{
  def apply(json: JsValue="""{}"""): DataFrameFilterSpell = new DataFrameFilterSpell().parseJson(json)
}