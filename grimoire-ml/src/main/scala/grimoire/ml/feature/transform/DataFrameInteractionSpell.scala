package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.feature.Interaction
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.HasInputCols
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by sjc505 on 17-6-21.
  */
class DataFrameInteractionSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasInputCols{
  val interaction = new Interaction()

  override protected def setup(dat: DataFrame): Boolean = {
    interaction
      .setInputCols($(inputCols).toArray)
      .setOutputCol($(outputCol))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    interaction.transform(dat)
  }
}

object DataFrameInteractionSpell{
  def apply(json: JsValue="""{}"""): DataFrameInteractionSpell =
    new DataFrameInteractionSpell().parseJson(json)
}
