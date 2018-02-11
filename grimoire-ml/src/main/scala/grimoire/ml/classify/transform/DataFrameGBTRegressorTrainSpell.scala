package grimoire.ml.classify.transform

/**
  * Created by jax on 17-6-27.
  */

import grimoire.configuration.param.HasMaxIter
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

class DataFrameGBTRegressorTrainSpell extends Spell[DataFrame,GBTRegressionModel]
  with HasLabelCol with HasFeaturesCol with HasMaxIter with HasPredictionCol with HasCacheNodeIds
  with HasCheckpointInterval with HasImpurity with HasLossType with HasMaxBins with HasMaxDepth
  with HasMaxMemoryInMB with HasMinInstancesPerNode with HasMinInfoGain with HasSeed with HasStepSize
  with HasSubsamplingRate{
  var gbt = new GBTRegressor()

  override def setup(dat: DataFrame): Boolean = {
    gbt
      .setLabelCol($(labelCol))
      .setFeaturesCol($(featuresCol))
      .setMaxIter($(maxIter))
      .setPredictionCol($(predictionCol))
      .setCacheNodeIds($(cacheNodeIds))
      .setCheckpointInterval($(checkpointInterval))
      .setImpurity($(impurity))
      .setLossType($(lossType))
      .setMaxBins($(maxBins))
      .setMaxDepth($(maxDepth))
      .setMaxMemoryInMB($(maxMemoryInMB))
      .setMinInstancesPerNode($(minInstancesPerNode))
      .setMinInfoGain($(minInfoGain))
      .setSeed($(seed))
      .setStepSize($(stepSize))
      .setSubsamplingRate($(subsamplingRate))
    super.setup ( dat )
  }

  override def transformImpl(dat: DataFrame): GBTRegressionModel = {
    gbt.fit(dat)
  }
}
object DataFrameGBTRegressorTrainSpell{
  def apply(json: JsValue="""{}"""): DataFrameGBTRegressorTrainSpell =
    new DataFrameGBTRegressorTrainSpell().parseJson(json)
}