package grimoire.ml.model

import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-3.
  */
class DataFrameCrossValidatorPredictSpell extends Spell[DataFrame & CrossValidatorModel,DataFrame]{


  override def transformImpl(dat: &[DataFrame, CrossValidatorModel]): DataFrame = {
    dat._2.transform(dat._1)
  }

}

object DataFrameCrossValidatorPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameCrossValidatorPredictSpell =
    new DataFrameCrossValidatorPredictSpell().parseJson(json)
}
