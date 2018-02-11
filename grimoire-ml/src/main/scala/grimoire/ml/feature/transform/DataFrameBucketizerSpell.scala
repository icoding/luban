package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.ml.configuration.param.HasSplits
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by Arno on 17-6-22.
  */
class DataFrameBucketizerSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasSplits{

  val bucketizer = new Bucketizer()

  override def setup(dat: DataFrame): Boolean = {
    bucketizer
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setSplits($(splits).toArray)
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    bucketizer.transform(dat)
  }

}

object DataFrameBucketizerSpell{
  def apply(json: JsValue="""{}"""): DataFrameBucketizerSpell =
    new DataFrameBucketizerSpell().parseJson(json)
}