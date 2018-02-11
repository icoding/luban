package grimoire.ml.classify.transform

import grimoire.configuration.param.HasOutputCol
import grimoire.dataset.&
import grimoire.ml.configuration.param.{HasFeatureCols, HasFeaturesCol, HasPredictionCol}
import grimoire.transform.Spell
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by Arno on 17-6-26.
  */
class DataFrameLogisticRegressionPredictSpell
  extends Spell[DataFrame & LogisticRegressionModel,DataFrame] {
  override def transformImpl(dat: DataFrame & LogisticRegressionModel): DataFrame = {
    dat._2.transform(dat._1)
  }

}

object DataFrameLogisticRegressionPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameLogisticRegressionPredictSpell =
    new DataFrameLogisticRegressionPredictSpell().parseJson(json)
}

