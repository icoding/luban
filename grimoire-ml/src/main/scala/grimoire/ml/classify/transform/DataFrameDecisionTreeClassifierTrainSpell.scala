package grimoire.ml.classify.transform


import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by Arno on 17-11-7.
  */
class DataFrameDecisionTreeClassifierTrainSpell
  extends Spell[DataFrame,DecisionTreeClassificationModel]
  with HasLabelCol with HasFeaturesCol with HasRawPredictionCol with HasSeed with HasCheckpointInterval
    with HasImpurity with HasMaxBins with HasMaxDepth with HasMinInfoGain
    with HasMinInstancesPerNode with HasPredictionCol with HasProbabilityCol with HasThresholds{

  val md = new DecisionTreeClassifier()

  override def setup(dat: DataFrame): Boolean = {
    md
      .setFeaturesCol($(featuresCol))
      .setLabelCol($(labelCol))
      .setPredictionCol($(predictionCol))
      .setProbabilityCol($(probabilityCol))
      .setRawPredictionCol($(rawPredictionCol))
      .setCheckpointInterval($(checkpointInterval))
      .setImpurity($(impurity))
      .setMaxBins($(maxBins))
      .setMaxDepth($(maxDepth))
      .setMinInfoGain($(minInfoGain))
      .setMinInstancesPerNode($(minInstancesPerNode))
      .setSeed($(seed))

    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DecisionTreeClassificationModel = {
    md.fit(dat)
  }
}

object DataFrameDecisionTreeClassifierTrainSpell{
  def apply(json: JsValue="""{}"""): DataFrameDecisionTreeClassifierTrainSpell =
    new DataFrameDecisionTreeClassifierTrainSpell().parseJson(json)
}
