package grimoire.ml.classify.transform

import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-5.
  */
class DataFrameDecisionTreeClassifierPredictSpell extends Spell[DataFrame & DecisionTreeClassificationModel,DataFrame]{

  override def transformImpl(dat: &[DataFrame, DecisionTreeClassificationModel]): DataFrame = {
    dat._2.transform(dat._1)
  }
}
object DataFrameDecisionTreeClassifierPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameDecisionTreeClassifierPredictSpell =
    new DataFrameDecisionTreeClassifierPredictSpell().parseJson(json)
}