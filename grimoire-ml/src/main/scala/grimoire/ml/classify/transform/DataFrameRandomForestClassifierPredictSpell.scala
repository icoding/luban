package grimoire.ml.classify.transform

import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.classification.{RandomForestClassificationModel}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-5.
  */
class DataFrameRandomForestClassifierPredictSpell extends Spell[DataFrame & RandomForestClassificationModel,DataFrame]{

  override def transformImpl(dat: &[DataFrame, RandomForestClassificationModel]): DataFrame = {
    dat._2.transform(dat._1)
  }
}
object DataFrameRandomForestClassifierPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameRandomForestClassifierPredictSpell =
    new DataFrameRandomForestClassifierPredictSpell().parseJson(json)
}