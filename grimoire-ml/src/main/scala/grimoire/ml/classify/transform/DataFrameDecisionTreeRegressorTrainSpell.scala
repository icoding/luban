package grimoire.ml.classify.transform

/**
  * Created by Arno on 17-11-8.
  */

import grimoire.configuration.param.{HasInputCol, HasMaxIter, HasOutputCol, HasRegParam}
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

class DataFrameDecisionTreeRegressorTrainSpell extends Spell[DataFrame,DecisionTreeRegressionModel]
  with HasLabelCol with HasFeaturesCol with HasPredictionCol with HasVarianceCol with HasCacheNodeIds
  with HasCheckpointInterval with HasImpurity with HasMaxBins with HasMaxDepth with HasMaxMemoryInMB
with HasMinInfoGain with HasMinInstancesPerNode with HasSeed
{
  var dt = new DecisionTreeRegressor()
  impurity.default("variance")
  seed.default(926680331)

  override def setup(dat: DataFrame): Boolean = {
    dt
      .setLabelCol($(labelCol))
      .setFeaturesCol($(featuresCol))
      .setPredictionCol($(predictionCol))
//      .setVarianceCol($(varianceCol))
      .setCacheNodeIds($(cacheNodeIds))
      .setCheckpointInterval($(checkpointInterval))
      .setImpurity($(impurity))
      .setMaxBins($(maxBins))
      .setMaxDepth($(maxDepth))
      .setMaxMemoryInMB($(maxMemoryInMB))
      .setMinInfoGain($(minInfoGain))
      .setMinInstancesPerNode($(minInstancesPerNode))
      .setSeed($(seed))

    super.setup ( dat )
  }

  override def transformImpl(dat: DataFrame): DecisionTreeRegressionModel = {
    dt.fit(dat)
  }
}
object DataFrameDecisionTreeRegressorTrainSpell{
  def apply(json: JsValue="""{}"""): DataFrameDecisionTreeRegressorTrainSpell =
    new DataFrameDecisionTreeRegressorTrainSpell().parseJson(json)
}