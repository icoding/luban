package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.ml.configuration.param.{HasBucketLength, HasNumHashTables}
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, MinHashLSH}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by sjc505 on 17-6-23.
  */
class DataFrameMinHashLSHSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasNumHashTables{
  val mh = new MinHashLSH()

  override def setup(dat: DataFrame): Boolean = {
    mh
      .setNumHashTables($(numHashTables))
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    mh.fit(dat).transform(dat)
  }
}

object DataFrameMinHashLSHSpell{
  def apply(json: JsValue="""{}"""): DataFrameMinHashLSHSpell =
    new DataFrameMinHashLSHSpell().parseJson(json)
}
