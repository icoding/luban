package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.dataset.&
import grimoire.Implicits._
import grimoire.transform.Spell
import org.apache.spark.ml.feature.IDFModel
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue


/**
  * Created by Arno on 2017/10/16.
  */
class DataFrameTFIDFVectorizeSpell extends Spell[DataFrame & IDFModel,DataFrame] {

  override def transformImpl(dat: DataFrame & IDFModel): DataFrame = {
    dat._2
      .transform(dat._1)
  }
}

object DataFrameTFIDFVectorizeSpell{
  def apply(json:JsValue="""{}"""): DataFrameTFIDFVectorizeSpell =
    new DataFrameTFIDFVectorizeSpell().parseJson(json)
}