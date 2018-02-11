package grimoire.ml.clustering.transform

/**
  * Created by jax on 17-6-28.
  */

import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.ml.clustering.{BisectingKMeans, BisectingKMeansModel}
import org.apache.spark.sql.DataFrame

class DataFrameBisectingKMeansSpell extends Spell[DataFrame,BisectingKMeansModel]
  with HasK with HasSeed with HasPredictionCol with HasFeaturesCol with HasMaxInter
  with HasMinDivisibleClusterSize {

  val bkm = new BisectingKMeans()
  seed.default(566573821)
  minDivisibleClusterSize.default(1)

  override def setup(dat: DataFrame): Boolean = {
    bkm
      .setK($(k))
      .setSeed($(seed))
      .setFeaturesCol($(featuresCol))
      .setPredictionCol($(predictionCol))
      .setMaxIter($(maxInter))
      .setMinDivisibleClusterSize($(minDivisibleClusterSize))

    super.setup ( dat )
  }

  override def transformImpl(dat: DataFrame): BisectingKMeansModel = {
    bkm.fit(dat)
  }

}

object DataFrameBisectingKMeansSpell{
  def apply(json: JsValue="""{}"""): DataFrameBisectingKMeansSpell =
    new DataFrameBisectingKMeansSpell().parseJson(json)
}