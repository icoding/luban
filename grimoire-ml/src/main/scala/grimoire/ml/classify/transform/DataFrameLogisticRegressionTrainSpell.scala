package grimoire.ml.classify.transform
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.configuration.param.{HasMaxIter, HasRegParam}
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.sql.DataFrame
/**
  * Created by Arno on 17-11-6.
  */
class DataFrameLogisticRegressionTrainSpell extends Spell[DataFrame,LogisticRegressionModel]
  with HasMaxIter with HasRegParam with HasElasticNetParam with HasFamily
  with HasLabelCol with HasFeaturesCol  with HasFitIntercept
  with HasStandardization  with HasThreshold with HasTol
  with HasProbabilityCol with HasRawPredictionCol with HasPredictionCol{

  val mlr = new LogisticRegression()
  elasticNetParam.default(0.0)
  fitIntercept.default(true)
  maxIter.default(100)
  probabilityCol.default("probability")
  regParam.default(0.0)
  rawPredictionCol.default("RawPredictionCol")

  override def setup(dat: DataFrame ): Boolean = {
    mlr
      .setMaxIter($(maxIter))
      .setRegParam($(regParam))
      .setElasticNetParam($(elasticNetParam))
      .setFamily($(family))
      .setLabelCol($(labelCol))
      .setFeaturesCol($(featuresCol))
      .setFitIntercept($(fitIntercept))
      .setStandardization($(standardization))
      .setThreshold($(threshold))
      .setTol($(tol))
      .setProbabilityCol($(probabilityCol))
      .setRawPredictionCol($(rawPredictionCol))
      .setPredictionCol($(predictionCol))
    super.setup(dat)
}
  override def transformImpl(dat: DataFrame): LogisticRegressionModel = {
    mlr.fit(dat)
  }
}

object DataFrameLogisticRegressionTrainSpell{
  def apply(json: JsValue="""{}"""): DataFrameLogisticRegressionTrainSpell =
    new DataFrameLogisticRegressionTrainSpell().parseJson(json)
}
