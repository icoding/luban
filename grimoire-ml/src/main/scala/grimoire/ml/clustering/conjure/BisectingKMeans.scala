package grimoire.ml.clustering.conjure
/**
  * Created by Aron on 17-11-20.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.clustering.transform.DataFrameBisectingKMeansSpell
import grimoire.ml.target.ModelTarget

object BisectingKMeans {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("BisectingKMeans").
      enableHiveSupport().
      getOrCreate()

    HiveSource(args(0)).
      cast(DataFrameBisectingKMeansSpell(args(1))).
      cast(ModelTarget(args(2))).
      conjure
  }

}
