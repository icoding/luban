package grimoire.ml.feature.transform

import grimoire.Implicits._
import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.ml.configuration.param.HasThreshold
import grimoire.spark.transform.dataframe.DataFrameSpell
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

/**
  * Created by Arno on 17-10-18.
  */
class DataFrameBinarizerSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasThreshold{

  val binarizer: Binarizer = new Binarizer()


  override def setup(dat: DataFrame): Boolean = {
    binarizer
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setThreshold($(threshold))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    binarizer.transform(dat)
  }
}


object DataFrameBinarizerSpell{
  def apply(json: JsValue="""{}"""): DataFrameBinarizerSpell =
    new DataFrameBinarizerSpell().parseJson(json)
}
