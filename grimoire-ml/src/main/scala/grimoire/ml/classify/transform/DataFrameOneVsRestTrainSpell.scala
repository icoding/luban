package grimoire.ml.classify.transform


import grimoire.ml.configuration.param._
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, OneVsRest, OneVsRestModel}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.dataset.&
import grimoire.transform.Spell

/**
  * Created by sjc505 on 17-6-29.
  */
class DataFrameOneVsRestTrainSpell
  extends Spell[DataFrame,OneVsRestModel]
  with HasFeaturesCol with HasLabelCol with HasPredictionCol{

  val ovr = new OneVsRest()
  val classifier = new LogisticRegression().setMaxIter(10).setTol(1E-6).setFitIntercept(true)

  override def setup(dat: DataFrame ): Boolean = {
    ovr
      .setFeaturesCol($(featuresCol))
      .setLabelCol($(labelCol))
      .setClassifier(classifier)
      .setPredictionCol($(predictionCol))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame ): OneVsRestModel = {
    ovr.fit(dat)
  }
}

object DataFrameOneVsRestTrainSpell{
  def apply(json: JsValue="""{}"""): DataFrameOneVsRestTrainSpell =
    new DataFrameOneVsRestTrainSpell().parseJson(json)
}
