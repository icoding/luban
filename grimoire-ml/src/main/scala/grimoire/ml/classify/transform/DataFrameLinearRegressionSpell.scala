package grimoire.ml.classify.transform

import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.configuration.param.{HasMaxIter, HasRegParam}
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.DataFrame
/**
  * Created by jax on 17-6-27.
  */

class DataFrameLinearRegressionTrainSpell
  extends Spell[DataFrame,LinearRegressionModel]
  with HasMaxIter with HasRegParam with HasElasticNetParam with HasFitIntercept
  with HasLabelCol with HasFeaturesCol with HasWeightCol with HasAggregationDepth
    with HasStandardization with HasSolver with HasTol with HasPredictionCol
{

  val mlr = new LinearRegression()
  maxIter.default(100)

  override def setup(dat: DataFrame): Boolean = {
    mlr
      .setLabelCol($(labelCol))
      .setFeaturesCol($(featuresCol))
//      .setWeightCol($(weightCol))
      .setPredictionCol($(predictionCol))
      .setMaxIter($(maxIter))
      .setRegParam($(regParam))
      .setElasticNetParam($(elasticNetParam))
      .setFitIntercept($(fitIntercept))
      .setStandardization($(standardization))
      .setTol($(tol))
      .setSolver($(solver))
      .setAggregationDepth($(aggregationDepth))

    super.setup ( dat )
  }

  override def transformImpl(dat: DataFrame): LinearRegressionModel = {
    mlr.fit(dat)
  }
}

object DataFrameLinearRegressionTrainSpell{
  def apply(json: JsValue="""{}"""): DataFrameLinearRegressionTrainSpell =
    new DataFrameLinearRegressionTrainSpell ().parseJson(json)
}