package grimoire.ml.classify.transform

import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.regression.GeneralizedLinearRegressionModel
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-5.
  */
class DataFrameGeneralizedLinearRegressionPredictSpell  extends Spell[DataFrame & GeneralizedLinearRegressionModel,DataFrame]
{

  override def transformImpl(data_model: DataFrame & GeneralizedLinearRegressionModel): DataFrame = {
    data_model._2.transform(data_model._1)
  }
}

object DataFrameGeneralizedLinearRegressionPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameGeneralizedLinearRegressionPredictSpell =
    new DataFrameGeneralizedLinearRegressionPredictSpell().parseJson(json)
}