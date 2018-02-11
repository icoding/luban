package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.HasDegree
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by sjc505 on 17-6-21.
  */
class DataFramePolynomiaExpansionSpell extends DataFrameSpell
  with HasInputCol with HasOutputCol with HasDegree{

  val polyExpansion = new PolynomialExpansion()

  override def setup(dat: DataFrame): Boolean = {
    polyExpansion
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setDegree($(degree))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    polyExpansion.transform(dat)
  }

}

object DataFramePolynomiaExpansionSpell{
  def apply(json: JsValue="""{}"""): DataFramePolynomiaExpansionSpell =
    new DataFramePolynomiaExpansionSpell().parseJson(json)
}
