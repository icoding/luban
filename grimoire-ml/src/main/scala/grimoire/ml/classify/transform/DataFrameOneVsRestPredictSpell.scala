package grimoire.ml.classify.transform

import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.classification. OneVsRestModel
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-5.
  */
class DataFrameOneVsRestPredictSpell extends Spell[DataFrame & OneVsRestModel,DataFrame]{

  override def transformImpl(dat: &[DataFrame, OneVsRestModel]): DataFrame = {
    dat._2.transform(dat._1)
  }
}
object DataFrameOneVsRestPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameOneVsRestPredictSpell =
    new DataFrameOneVsRestPredictSpell().parseJson(json)
}
