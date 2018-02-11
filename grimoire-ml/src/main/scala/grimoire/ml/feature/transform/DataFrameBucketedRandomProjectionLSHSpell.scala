package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.ml.configuration.param.{HasBucketLength, HasNumHashTables}
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by sjc505 on 17-6-23.
  */
class DataFrameBucketedRandomProjectionLSHSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasBucketLength with HasNumHashTables{
  val brp = new BucketedRandomProjectionLSH()

  override def setup(dat: DataFrame): Boolean = {
    brp
      .setBucketLength($(bucketLength))
      .setNumHashTables($(numHashTables))
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    brp.fit(dat).transform(dat)
  }
}

object DataFrameBucketedRandomProjectionLSHSpell{
  def apply(json: JsValue="""{}"""): DataFrameBucketedRandomProjectionLSHSpell =
    new DataFrameBucketedRandomProjectionLSHSpell().parseJson(json)
}
