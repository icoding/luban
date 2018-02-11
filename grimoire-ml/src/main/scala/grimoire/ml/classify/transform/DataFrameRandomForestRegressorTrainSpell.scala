package grimoire.ml.classify.transform

import grimoire.Implicits._
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

/**
  * Created by jax on 17-6-28.
  */
class DataFrameRandomForestRegressorTrainSpell extends Spell[DataFrame,RandomForestRegressionModel]
  with HasLabelCol with HasFeaturesCol with HasPredictionCol with HasCacheNodeIds with HasCheckpointInterval
  with HasSeed with HasImpurity with HasMaxBins with HasMaxDepth with HasMaxMemoryInMB
  with HasMinInfoGain with HasMinInstancesPerNode with HasNumTrees with HasSubsamplingRate
  with HasFeatureSubsetStrategy
{
  var rf = new RandomForestRegressor()
  impurity.default("variance")
  seed.default(235498149)

  override def setup(dat: DataFrame): Boolean = {
    rf
      .setLabelCol($(labelCol))
      .setFeaturesCol($(featuresCol))
      .setPredictionCol($(predictionCol))
      .setCacheNodeIds($(cacheNodeIds))
      .setCheckpointInterval($(checkpointInterval))
      .setFeatureSubsetStrategy($(featureSubsetStrategy))
      .setImpurity($(impurity))
      .setMaxBins($(maxBins))
      .setMaxDepth($(maxDepth))
      .setMaxMemoryInMB($(maxMemoryInMB))
      .setMinInfoGain($(minInfoGain))
      .setMinInstancesPerNode($(minInstancesPerNode))
      .setNumTrees($(numTrees))
      .setSeed($(seed))
      .setSubsamplingRate($(subsamplingRate))

    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): RandomForestRegressionModel = {
    rf.fit(dat)
  }
}
object DataFrameRandomForestRegressorTrainSpell{
  def apply(json: JsValue="""{}"""): DataFrameRandomForestRegressorTrainSpell =
    new DataFrameRandomForestRegressorTrainSpell().parseJson(json)
}