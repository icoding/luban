package grimoire.ml.classify.transform

import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-5.
  */
class DataFrameMultilayerPerceptronClassifierPredictSpell extends Spell[DataFrame & MultilayerPerceptronClassificationModel,DataFrame]{

  override def transformImpl(dat: &[DataFrame, MultilayerPerceptronClassificationModel]): DataFrame = {
    dat._2.transform(dat._1)
  }
}
object DataFrameMultilayerPerceptronClassifierPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameMultilayerPerceptronClassifierPredictSpell =
    new DataFrameMultilayerPerceptronClassifierPredictSpell().parseJson(json)
}