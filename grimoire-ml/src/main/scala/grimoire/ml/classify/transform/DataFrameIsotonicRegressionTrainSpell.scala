package grimoire.ml.classify.transform

/**
  * Created by jax on 17-6-28.
  */

import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.ml.regression.{IsotonicRegression, IsotonicRegressionModel}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

class DataFrameIsotonicRegressionTrainSpell extends Spell[DataFrame,IsotonicRegressionModel]
  with HasFeaturesCol with HasLabelCol with HasIsotonic with HasPredictionCol with HasWeightCol
 with HasFeatureIndex{
  var Isotonic = new IsotonicRegression()

  override def setup(dat: DataFrame): Boolean = {
    Isotonic
      .setFeaturesCol($(featuresCol))
      .setLabelCol($(labelCol))
      .setIsotonic($(isotonic))
      .setPredictionCol($(predictionCol))
      .setWeightCol($(weightCol))
      .setFeatureIndex($(featureIndex))

    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): IsotonicRegressionModel = {
    Isotonic.fit(dat)
  }
}
object DataFrameIsotonicRegressionTrainSpell {
  def apply(json: JsValue ="""{}"""): DataFrameIsotonicRegressionTrainSpell =
    new DataFrameIsotonicRegressionTrainSpell ().parseJson ( json )
}
