package grimoire.ml.classify.transform

import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.configuration.param.{HasMaxIter, HasRegParam}
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.ml.regression.{GeneralizedLinearRegression, GeneralizedLinearRegressionModel}
import org.apache.spark.sql.DataFrame


/**
  * Created by jax on 17-6-27.
  */

class DataFrameGeneralizedLinearRegressionTrainSpell
  extends Spell[DataFrame,GeneralizedLinearRegressionModel]
  with HasFamily with HasLink with HasMaxIter with HasRegParam
  with HasLabelCol with HasFeaturesCol with  HasWeightCol with HasSolver
    with HasTol with HasLinkPredictionCol with HasPredictionCol{

  val glr = new GeneralizedLinearRegression()

  override def setup(dat: DataFrame): Boolean = {
    glr
      .setLabelCol($(labelCol))
      .setFeaturesCol($(featuresCol))
      .setLinkPredictionCol($(linkPredictionCol))
      .setWeightCol($(weightCol))
      .setPredictionCol($(predictionCol))
      .setFamily($(family))
      .setLink($(link))
      .setMaxIter($(maxIter))
      .setRegParam($(regParam))
      .setSolver($(solver))
      .setTol($(tol))


    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): GeneralizedLinearRegressionModel = {
    glr.fit(dat)
  }

}

object DataFrameGeneralizedLinearRegressionTrainSpell{
  def apply(json: JsValue="""{}"""): DataFrameGeneralizedLinearRegressionTrainSpell =
    new DataFrameGeneralizedLinearRegressionTrainSpell().parseJson(json)
}