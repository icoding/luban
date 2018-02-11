package grimoire.ml.classify.transform

import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.classification.GBTClassificationModel
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
/**
  * Created by sjc505 on 17-7-5.
  */
class DataFrameGBTClassifierPredictSpell extends Spell[DataFrame & GBTClassificationModel,DataFrame]{

  override def transformImpl(dat: &[DataFrame, GBTClassificationModel]): DataFrame = {
    dat._2.transform(dat._1)
  }
}
object DataFrameGBTClassifierPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameGBTClassifierPredictSpell =
    new DataFrameGBTClassifierPredictSpell().parseJson(json)
}