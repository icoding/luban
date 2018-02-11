package clustering
/**
  * Created by Aron on 17-11-8.
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
      master("local[4]").
      appName("BisectingKMeans").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"BisectingKMeans"}""").
      cast(DataFrameBisectingKMeansSpell("""{"LabelCol":"label","FeaturesCol":"features","PredictionCol":"prediction","K":2}""")).
      cast(ModelTarget("""{"OutputModelTarget":"hdfs:///user/model/BisectingKMeans","Overwrite":true}""")).
      conjure
  }

}
