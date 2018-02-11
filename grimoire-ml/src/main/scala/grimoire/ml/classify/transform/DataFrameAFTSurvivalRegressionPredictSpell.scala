package grimoire.ml.classify.transform

import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.regression.AFTSurvivalRegressionModel
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-5.
  */
class DataFrameAFTSurvivalRegressionPredictSpell extends Spell[DataFrame & AFTSurvivalRegressionModel,DataFrame]{

  override def transformImpl(dat: &[DataFrame, AFTSurvivalRegressionModel]): DataFrame = {
    dat._2.transform(dat._1)
  }
}
object DataFrameAFTSurvivalRegressionPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameAFTSurvivalRegressionPredictSpell =
    new DataFrameAFTSurvivalRegressionPredictSpell().parseJson(json)
}
