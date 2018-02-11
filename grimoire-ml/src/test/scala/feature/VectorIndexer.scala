package feature
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameVectorIndexerSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object VectorIndexer {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("VectorIndexer").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"VectorIndexer"}""").
      cast(DataFrameVectorIndexerSpell("""{"InputCol":"features","OutputCol":"indexed","MaxCategories":10}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"VectorIndexerConjure"}""")).
      conjure
  }
}
