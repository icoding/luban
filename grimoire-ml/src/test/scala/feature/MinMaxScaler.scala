package feature
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameMinMaxScalerSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object MinMaxScaler {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("MinMaxScaler").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"MinMaxScaler"}""").
      cast(DataFrameMinMaxScalerSpell("""{"InputCol":"features","OutputCol":"scaledFeatures"}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"MinMaxScalerConjure"}""")).
      conjure
  }
}
