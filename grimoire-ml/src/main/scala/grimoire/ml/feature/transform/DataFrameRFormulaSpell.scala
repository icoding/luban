package grimoire.ml.feature.transform

import grimoire.ml.configuration.param.{HasFeaturesCol, HasFormula, HasLabelCol}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by sjc505 on 17-6-23.
  */
class DataFrameRFormulaSpell extends DataFrameSpell with HasLabelCol
  with HasFormula with  HasFeaturesCol{
  val formu = new RFormula()

  override def setup(dat: DataFrame): Boolean = {

    formu
      .setFormula($(formula))
      .setFeaturesCol($(featuresCol))
      .setLabelCol($(labelCol))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    formu.fit(dat).transform(dat)
  }
}

object DataFrameRFormulaSpell{
  def apply(json: JsValue="""{}"""): DataFrameRFormulaSpell =
    new DataFrameRFormulaSpell().parseJson(json)
}