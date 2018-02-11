package grimoire.ml.feature.transform

import grimoire.configuration.param.HasOutputCol
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.{HasFeaturesCol, HasLabelCol, HasNumTopFeatures}
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by Arno on 17-10-19.
  */
class DataFrameChiSqSelectorSpell extends DataFrameSpell with HasOutputCol
  with HasFeaturesCol with HasLabelCol with HasNumTopFeatures{

  val selector = new ChiSqSelector()
  numTopFeatures.default(50)

  override def setup(dat: DataFrame): Boolean = {
    selector
      .setNumTopFeatures($(numTopFeatures))
      .setFeaturesCol($(featuresCol))
      .setLabelCol($(labelCol))
      .setOutputCol($(outputCol))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    selector.fit(dat).transform(dat)
  }
}

object DataFrameChiSqSelectorSpell{
  def apply(json: JsValue="""{}"""): DataFrameChiSqSelectorSpell =
    new DataFrameChiSqSelectorSpell().parseJson(json)
}