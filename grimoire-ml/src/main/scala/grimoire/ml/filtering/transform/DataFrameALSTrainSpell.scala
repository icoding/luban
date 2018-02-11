package grimoire.ml.filtering.transform


import grimoire.Implicits._
import grimoire.configuration.param.{HasMaxIter, HasRegParam}
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
/**
  * Created by sjc505 on 17-6-29.
  */

class DataFrameALSTrainSpell extends Spell[DataFrame,ALSModel]
  with HasMaxIter with HasRegParam with HasUserCol with HasItemCol with HasRatingCol
  with HasPredictionCol with HasAlpha with HasCheckpointInterval with HasFinalStorageLevel
  with HasNonnegative with HasNumBlocks with HasNumItemBlocks with HasRank with HasSeed with HasImplicitPrefs {

  val als = new ALS()
  maxIter.default(10)
  regParam.default(0.1)
  userCol.default("user")
  itemCol.default("item")
  ratingCol.default("rating")
  seed.default(1994790107)

  override def setup(dat: DataFrame): Boolean = {
    als
      .setMaxIter($(maxIter))
      .setRegParam($(regParam))
      .setUserCol($(userCol))
      .setItemCol($(itemCol))
      .setRatingCol($(ratingCol))
      .setPredictionCol($(predictionCol))
      .setAlpha($(alpha))
      .setCheckpointInterval($(checkpointInterval))
      .setFinalStorageLevel($(finalStorageLevel))
      .setNonnegative($(nonnegative))
      .setNumBlocks($(numBlocks))
      .setNumItemBlocks($(numItemBlocks))
      .setRank($(rank))
      .setSeed($(seed))
      .setImplicitPrefs($(implicitPrefs))

    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): ALSModel = {
     als.fit(dat)
  }
}

object DataFrameALSTrainSpell{
  def apply(json: JsValue="""{}"""): DataFrameALSTrainSpell =
    new DataFrameALSTrainSpell().parseJson(json)
}
