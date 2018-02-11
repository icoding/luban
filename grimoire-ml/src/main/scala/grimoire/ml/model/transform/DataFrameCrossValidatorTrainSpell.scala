package grimoire.ml.model

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.{HasNumFolds, HasSeed}
import grimoire.transform.Spell
import org.apache.spark.ml.classification.LogisticRegression

/**
  * Created by sjc505 on 17-6-30.
  */
class DataFrameCrossValidatorTrainSpell extends Spell[DataFrame,CrossValidatorModel]
  with HasNumFolds with HasSeed{

  val cv = new CrossValidator()
  val lr = new LogisticRegression().setMaxIter(10)

  val paramGrid = new ParamGridBuilder()
    .addGrid(lr.regParam, Array(0.1, 0.01))
    .build()


  override def setup(dat: DataFrame): Boolean = {
      cv
        .setEstimator(lr)
        .setEstimatorParamMaps(paramGrid)
        .setEvaluator(new BinaryClassificationEvaluator)
        .setNumFolds($(numFolds))
        .setSeed($(seed))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): CrossValidatorModel = {
    cv.fit(dat)
  }

}

object DataFrameCrossValidatorTrainSpell{
  def apply(json: JsValue="""{}"""): DataFrameCrossValidatorTrainSpell =
    new DataFrameCrossValidatorTrainSpell().parseJson(json)
}