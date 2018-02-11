package grimoire.ml.clustering.transform

/**
  * Created by jax on 17-6-28.
  */

import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.DataFrame

class DataFrameKMeansSpell extends Spell[DataFrame,KMeansModel]
  with HasK with HasSeed with HasFeaturesCol with HasPredictionCol
  with HasMaxInter with HasTol with HasInitMode with HasInitSteps
{

  val kmeans = new KMeans()

  override def setup(dat: DataFrame): Boolean = {
    kmeans
      .setFeaturesCol($(featuresCol))
      .setPredictionCol($(predictionCol))
      .setK($(k))
      .setSeed($(seed))
      .setMaxIter($(maxInter))
      .setTol($(tol))
      .setInitMode($(initMode))
      .setInitSteps($(initSteps))

    super.setup ( dat )
  }

  override def transformImpl(dat: DataFrame): KMeansModel = {
    kmeans.fit(dat)
  }

}

object DataFrameKMeansSpell{
  def apply(json: JsValue="""{}"""): DataFrameKMeansSpell =
    new DataFrameKMeansSpell ().parseJson(json)
}