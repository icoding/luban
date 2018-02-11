package feature
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameStandardScalerSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object StandardScaler {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("StandardScaler").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"StandardScaler"}""").
      cast(DataFrameStandardScalerSpell("""{"InputCol":"features","OutputCol":"scaledFeatures","WithStd":true}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"StandardScalerConjure"}""")).
      conjure
  }
}
