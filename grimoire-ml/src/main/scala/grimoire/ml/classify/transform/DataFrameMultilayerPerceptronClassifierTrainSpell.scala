package grimoire.ml.classify.transform

import grimoire.configuration.param.HasMaxIter
import grimoire.ml.configuration.param._
import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.transform.Spell

/**
  * Created by sjc505 on 17-6-28.
  */
class DataFrameMultilayerPerceptronClassifierTrainSpell  extends Spell[DataFrame,MultilayerPerceptronClassificationModel]
  with HasLabelCol with HasFeaturesCol with HasLayers with HasSeed with HasMaxIter with HasBlockSize with HasTol with HasStepSize{

  val rf = new MultilayerPerceptronClassifier()

  override def setup(dat: DataFrame ): Boolean = {
    rf
      .setLabelCol($(labelCol))
      .setFeaturesCol($(featuresCol))
      .setLayers($(layers).toArray)
      .setBlockSize($(blockSize))
      .setSeed($(seed))
      .setMaxIter($(maxIter))
      .setTol($(tol))
      .setStepSize($(stepSize))

    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): MultilayerPerceptronClassificationModel = {
    rf.fit(dat)
  }
}

object DataFrameMultilayerPerceptronClassifierTrainSpell{
  def apply(json: JsValue="""{}"""): DataFrameMultilayerPerceptronClassifierTrainSpell =
    new DataFrameMultilayerPerceptronClassifierTrainSpell().parseJson(json)
}

