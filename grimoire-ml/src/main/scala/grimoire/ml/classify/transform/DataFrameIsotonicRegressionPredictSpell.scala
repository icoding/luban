package grimoire.ml.classify.transform

import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.regression.IsotonicRegressionModel
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-5.
  */
class DataFrameIsotonicRegressionPredictSpell  extends Spell[DataFrame & IsotonicRegressionModel,DataFrame]
{
  override def transformImpl(data_model: DataFrame & IsotonicRegressionModel): DataFrame = {
    data_model._2.transform(data_model._1)
  }
}

object DataFrameIsotonicRegressionPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameIsotonicRegressionPredictSpell =
    new DataFrameIsotonicRegressionPredictSpell().parseJson(json)
}