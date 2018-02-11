package grimoire.ml.classify.transform

/**
  * Created by Aron on 17-6-28.
  */

import grimoire.Implicits._
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.ml.regression.{AFTSurvivalRegression, AFTSurvivalRegressionModel}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

class DataFrameAFTSurvivalRegressionTrainSpell extends Spell[DataFrame,AFTSurvivalRegressionModel]
  with HasQuantileProbabilities with HasQuantilesCol with HasAggregationDepth with HasCensorCol
  with HasFeaturesCol with HasLabelCol with HasPredictionCol with HasMaxInter with HasFitIntercept with HasTol
{
  var aft = new AFTSurvivalRegression()

  override def setup(dat: DataFrame): Boolean = {
    aft
      .setQuantileProbabilities($(quantileProbabilities).toArray)
      .setQuantilesCol($(quantilesCol))
      .setAggregationDepth($(aggregationDepth))
      .setCensorCol($(censorCol))
      .setFeaturesCol($(featuresCol))
      .setLabelCol($(labelCol))
      .setPredictionCol($(predictionCol))
      .setMaxIter($(maxInter))
      .setFitIntercept($(fitIntercept))
      .setTol($(tol))

    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): AFTSurvivalRegressionModel = {
    aft.fit(dat)
  }
}


object DataFrameAFTSurvivalRegressionTrainSpell {
  def apply(json: JsValue ="""{}"""): DataFrameAFTSurvivalRegressionTrainSpell =
    new DataFrameAFTSurvivalRegressionTrainSpell().parseJson ( json )
}