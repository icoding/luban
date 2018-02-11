package grimoire.ml.clustering.conjure
/**
  * Created by Aron on 17-11-20.
  */
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.clustering.source.BisectingKMeansModelSource
import grimoire.ml.clustering.transform.DataFrameBisectingKMeansPreSpell

object BisectingKMeansPre {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("BisectingKMeansPre").
      enableHiveSupport().
      getOrCreate()

    (HiveSource(args(0)) :+ BisectingKMeansModelSource(args(1))).
      cast(DataFrameBisectingKMeansPreSpell()).
      cast(DataFrameTarget(args(2))).conjure
  }
}