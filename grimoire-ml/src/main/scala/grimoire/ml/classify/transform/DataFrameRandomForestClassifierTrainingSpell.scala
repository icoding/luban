package grimoire.ml.classify.transform

import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by Arno on 17-11-7.
  */
class DataFrameRandomForestClassifierTrainSpell extends Spell[DataFrame,RandomForestClassificationModel]
  with HasLabelCol with HasFeaturesCol with HasNumTrees with HasImpurity with HasMaxBins with HasMaxDepth with HasMinInfoGain
  with HasCheckpointInterval with HasMinInstancesPerNode with HasPredictionCol with HasFeatureSubsetStrategy
  with HasProbabilityCol with HasThresholds with HasSeed with HasRawPredictionCol with HasSubsamplingRate{

  val rf = new RandomForestClassifier()
  numTrees.default(20)

  override def setup(dat: DataFrame ): Boolean = {
    rf
      .setLabelCol($(labelCol))
      .setFeaturesCol($(featuresCol))
      .setNumTrees($(numTrees))
      .setPredictionCol($(predictionCol))
      .setProbabilityCol($(probabilityCol))
      .setRawPredictionCol($(rawPredictionCol))
      .setImpurity($(impurity))
      .setCheckpointInterval($(checkpointInterval))
      .setFeatureSubsetStrategy($(featureSubsetStrategy))
      .setSubsamplingRate($(subsamplingRate))
      .setMaxBins($(maxBins))
      .setMaxDepth($(maxDepth))
      .setMinInfoGain($(minInfoGain))
      .setMinInstancesPerNode($(minInstancesPerNode))
      .setSeed($(seed))
//      .setThresholds($(thresholds).toArray)

    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): RandomForestClassificationModel = {
    rf.fit(dat)
  }
}

object DataFrameRandomForestClassifierTrainSpell{
  def apply(json: JsValue="""{}"""): DataFrameRandomForestClassifierTrainSpell =
    new DataFrameRandomForestClassifierTrainSpell().parseJson(json)
}
