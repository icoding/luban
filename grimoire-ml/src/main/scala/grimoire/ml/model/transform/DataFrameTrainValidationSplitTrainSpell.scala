package grimoire.ml.model

import grimoire.ml.configuration.param.{HasSeed, HasTrainRatio}
import grimoire.transform.Spell
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import org.apache.spark.ml.classification.LogisticRegression

/**
  * Created by sjc505 on 17-7-3.
  */
class DataFrameTrainValidationSplitTrainSpell extends Spell[DataFrame,TrainValidationSplitModel]
  with HasTrainRatio with HasSeed{

  val trainValidationSplit = new TrainValidationSplit()
  val lr = new LogisticRegression().setMaxIter(10)

  val paramGrid = new ParamGridBuilder()
    .addGrid(lr.regParam, Array(0.1, 0.01))
    .addGrid(lr.fitIntercept)
    .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
    .build()



  override def setup(dat: DataFrame): Boolean = {
    trainValidationSplit
      .setEstimator(lr)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(new RegressionEvaluator)
      .setTrainRatio($(trainRatio))
      .setSeed($(seed))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): TrainValidationSplitModel = {
    trainValidationSplit.fit(dat)
  }

}

object DataFrameTrainValidationSplitTrainSpell{
  def apply(json: JsValue="""{}"""): DataFrameTrainValidationSplitTrainSpell =
    new DataFrameTrainValidationSplitTrainSpell().parseJson(json)
}