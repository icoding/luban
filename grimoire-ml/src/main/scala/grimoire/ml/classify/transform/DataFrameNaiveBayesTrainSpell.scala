package grimoire.ml.classify.transform

import grimoire.ml.configuration.param._
import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel, RandomForestClassifier}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.transform.Spell

/**
  * Created by sjc505 on 17-6-28.
  */
class DataFrameNaiveBayesTrainSpell extends Spell[DataFrame,NaiveBayesModel]
  with HasLabelCol with HasFeaturesCol with HasWeightCol with HasModelType with HasSmothing
  with HasPredictionCol with HasProbabilityCol with HasRawPredictionCol with HasThresholds{

  val nb = new NaiveBayes()

  override def setup(dat: DataFrame ): Boolean = {
    nb
      .setLabelCol($(labelCol))
      .setFeaturesCol($(featuresCol))
//      .setWeightCol($(weightCol))
      .setPredictionCol($(predictionCol))
      .setRawPredictionCol($(rawPredictionCol))
      .setProbabilityCol($(probabilityCol))
//      .setThresholds($(thresholds).toArray)
      .setModelType($(modelType))
      .setSmoothing($(smothing))

    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): NaiveBayesModel = {
    nb.fit(dat)
  }
}

object DataFrameNaiveBayesTrainSpell{
  def apply(json: JsValue="""{}"""): DataFrameNaiveBayesTrainSpell =
    new DataFrameNaiveBayesTrainSpell().parseJson(json)
}

