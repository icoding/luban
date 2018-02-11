package grimoire.ml.filtering.transform

import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by Arno on 17-11-8.
  */
class DataFrameALSPredictSpell
  extends Spell[DataFrame & ALSModel,DataFrame] {
  override def transformImpl(dat: &[DataFrame, ALSModel]): DataFrame = {
    dat._2.transform(dat._1)
  }
}

object DataFrameALSPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameALSPredictSpell =
    new DataFrameALSPredictSpell().parseJson(json)
}

