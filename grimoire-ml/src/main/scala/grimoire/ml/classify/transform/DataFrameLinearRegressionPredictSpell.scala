package grimoire.ml.classify.transform

/**
  * Created by jax on 17-7-3.
  */

import grimoire.Implicits._
import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

class DataFrameLinearRegressionPredictSpell
  extends Spell[DataFrame & LinearRegressionModel,DataFrame]
{

  override def transformImpl(data_model: DataFrame & LinearRegressionModel): DataFrame = {
    data_model._2.transform(data_model._1)
  }
}

object DataFrameLinearRegressionPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameLinearRegressionPredictSpell =
    new DataFrameLinearRegressionPredictSpell().parseJson(json)
}