package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.HasHandleInvalid
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by Arno on 17-10-19.
  */
class DataFrameStringIndexerSpell extends DataFrameSpell with HasInputCol with HasOutputCol
  with HasHandleInvalid
{
  val indexer = new StringIndexer()
  handleInvalid.default("error")

  override protected def setup(dat: DataFrame): Boolean = {
    indexer
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setHandleInvalid($(handleInvalid))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    val model =indexer.fit(dat)
    model.transform(dat)
  }

}

object DataFrameStringIndexerSpell{
  def apply(json: JsValue="""{}"""): DataFrameStringIndexerSpell =
    new DataFrameStringIndexerSpell().parseJson(json)
}