package grimoire.ml.clustering.transform

/**
  * Created by jax on 17-6-28.
  */
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.ml.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.sql.DataFrame

class DataFrameGMMSpell extends Spell[DataFrame,GaussianMixtureModel]
  with HasK with HasFeaturesCol with HasMaxInter with HasPredictionCol
  with HasProbabilityCol with HasSeed with HasTol {

  val gmm = new GaussianMixture()

  override def setup(dat: DataFrame): Boolean = {
    gmm
      .setFeaturesCol($(featuresCol))
      .setPredictionCol($(predictionCol))
      .setProbabilityCol($(probabilityCol))
      .setK($(k))
      .setMaxIter($(maxInter))
      .setSeed($(seed))
      .setTol($(tol))
    super.setup ( dat )
  }

  override def transformImpl(dat: DataFrame): GaussianMixtureModel = {
    gmm.fit(dat)
  }

}

object DataFrameGMMSpell{
  def apply(json: JsValue="""{}"""): DataFrameGMMSpell =
    new DataFrameGMMSpell ().parseJson(json)
}