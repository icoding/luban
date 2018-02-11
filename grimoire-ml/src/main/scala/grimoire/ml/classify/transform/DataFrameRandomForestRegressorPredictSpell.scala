package grimoire.ml.classify.transform

import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-5.
  */
class DataFrameRandomForestRegressorPredictSpell extends Spell[DataFrame & RandomForestRegressionModel,DataFrame]{

  override def transformImpl(dat: &[DataFrame, RandomForestRegressionModel]): DataFrame = {
    dat._2.transform(dat._1)
  }
}
object DataFrameRandomForestRegressorPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameRandomForestRegressorPredictSpell =
    new DataFrameRandomForestRegressorPredictSpell().parseJson(json)
}