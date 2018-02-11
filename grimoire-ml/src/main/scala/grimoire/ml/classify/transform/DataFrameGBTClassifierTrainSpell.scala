package grimoire.ml.classify.transform

import grimoire.ml.configuration.param._
import org.apache.spark.ml.classification.{DecisionTreeClassifier, GBTClassificationModel, GBTClassifier}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.configuration.param.HasMaxIter
import grimoire.transform.Spell

/**
  * Created by Arno on 17-11-7.
  */
class DataFrameGBTClassifierTrainSpell extends Spell[DataFrame,GBTClassificationModel]
  with HasLabelCol with HasFeaturesCol with  HasMaxIter with HasImpurity with HasMaxBins with HasMaxDepth with HasMinInfoGain
  with HasCheckpointInterval with HasMinInstancesPerNode with HasPredictionCol
  with HasSeed with HasRawPredictionCol with HasSubsamplingRate with HasLossType with HasStepSize{


  val mdt = new GBTClassifier()
  seed.default(-1287390502)
  stepSize.default(0.1)
  lossType.default("logistic")

  override def setup(dat: DataFrame): Boolean = {
    mdt
      .setFeaturesCol($(featuresCol))
      .setLabelCol($(labelCol))
      .setPredictionCol($(predictionCol))
      .setImpurity($(impurity))
      .setCheckpointInterval($(checkpointInterval))
      .setSubsamplingRate($(subsamplingRate))
      .setMaxBins($(maxBins))
      .setMaxDepth($(maxDepth))
      .setMaxIter($(maxIter))
      .setMinInfoGain($(minInfoGain))
      .setMinInstancesPerNode($(minInstancesPerNode))
      .setSeed($(seed))
      .setLossType($(lossType))
      .setStepSize($(stepSize))

    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): GBTClassificationModel = {
    mdt.fit(dat)
  }
}

object DataFrameGBTClassifierTrainSpell{
  def apply(json: JsValue="""{}"""): DataFrameGBTClassifierTrainSpell =
    new DataFrameGBTClassifierTrainSpell().parseJson(json)
}

