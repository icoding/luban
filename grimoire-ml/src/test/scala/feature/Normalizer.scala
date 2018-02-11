package feature
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameNormalizerSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object Normalizer {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("Normalizer").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"Normalizer"}""").
      cast(DataFrameNormalizerSpell("""{"InputCol":"features","OutputCol":"normFeatures","P":1}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"NormalizerConjure"}""")).
      conjure
  }
}
