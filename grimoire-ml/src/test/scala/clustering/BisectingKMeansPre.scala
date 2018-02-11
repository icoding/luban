package clustering
/**
  * Created by Aron on 17-11-8.
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
      master("local[1]").
      enableHiveSupport().
      getOrCreate()

    (HiveSource("""{"InputHiveTable":"BisectingKMeans"}""") :+ BisectingKMeansModelSource("""{"InputModelSource":"hdfs:///user/model/BisectingKMeans"}""")).
      cast(DataFrameBisectingKMeansPreSpell()).
      cast(DataFrameTarget("""{"OutputHiveTable":"BisectingKMeansPre"}""")).conjure
  }
}
