package grimoire.ml.classify.transform

import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
/**
  * Created by sjc505 on 17-7-5.
  */
class DataFrameGBTRegressorPredictSpell extends Spell[DataFrame & GBTRegressionModel,DataFrame]{

  override def transformImpl(dat: &[DataFrame, GBTRegressionModel]): DataFrame = {
    dat._2.transform(dat._1)
  }
}
object DataFrameGBTRegressorPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameGBTRegressorPredictSpell =
    new DataFrameGBTRegressorPredictSpell().parseJson(json)
}