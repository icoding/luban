package grimoire.ml.filtering.conjure

/**
  * Created by Aron on 17-11-8.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.filtering.transform.DataFrameALSTrainSpell
import grimoire.ml.target.ModelTarget

object ALS {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("ALS").
      enableHiveSupport().
      getOrCreate()

    HiveSource(args(0)).
      cast(DataFrameALSTrainSpell(args(1))).
      cast(ModelTarget(args(2))).
      conjure
  }

}