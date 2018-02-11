package grimoire.ml.model

import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.tuning.{TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
/**
  * Created by sjc505 on 17-7-3.
  */
class DataFrameTrainValidationSplitPredictSpell extends Spell[DataFrame & TrainValidationSplitModel,DataFrame]{


  override def transformImpl(dat: &[DataFrame, TrainValidationSplitModel]): DataFrame = {
    dat._2.transform(dat._1)
  }

}
object DataFrameTrainValidationSplitPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameTrainValidationSplitPredictSpell =
    new DataFrameTrainValidationSplitPredictSpell().parseJson(json)
}