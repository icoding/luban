package grimoire.ml.classify.transform

import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-5.
  */
class DataFrameDecisionTreeRegressorPredictSpell  extends Spell[DataFrame & DecisionTreeRegressionModel,DataFrame]{

  override def transformImpl(dat: &[DataFrame, DecisionTreeRegressionModel]): DataFrame = {
    dat._2.transform(dat._1)
  }
}
object DataFrameDecisionTreeRegressorPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameDecisionTreeRegressorPredictSpell =
    new DataFrameDecisionTreeRegressorPredictSpell().parseJson(json)
}
