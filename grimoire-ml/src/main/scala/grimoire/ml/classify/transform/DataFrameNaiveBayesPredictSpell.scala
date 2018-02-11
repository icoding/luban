package grimoire.ml.classify.transform

import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
/**
  * Created by sjc505 on 17-7-5.
  */
class DataFrameNaiveBayesPredictSpell extends Spell[DataFrame & NaiveBayesModel,DataFrame]{

  override def transformImpl(dat: &[DataFrame, NaiveBayesModel]): DataFrame = {
    dat._2.transform(dat._1)
  }
}
object DataFrameNaiveBayesPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameNaiveBayesPredictSpell =
    new DataFrameNaiveBayesPredictSpell().parseJson(json)
}
