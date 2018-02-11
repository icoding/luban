package grimoire.ml.feature.conjure
/**
  * Created by Aron on 17-11-20.
  */
import grimoire.ml.feature.source.IDFModelSource
import grimoire.ml.feature.transform.DataFrameTFIDFVectorizeSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object TFIDFVectorize {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("TFIDFVectorize").
      enableHiveSupport().
      getOrCreate()

    (HiveSource(args(0)) :+ IDFModelSource(args(1))).
      cast(DataFrameTFIDFVectorizeSpell()).
      cast(DataFrameTarget(args(2))).conjure
  }
}
