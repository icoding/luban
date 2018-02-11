package feature
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameBucketizerSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object Bucketizer {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("Bucketizer").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"Bucketizer"}""").
      cast(DataFrameBucketizerSpell("""{"InputCol":"features","OutputCol":"bucketedFeatures","Splits":[-1000, -0.5, 0.0, 0.5, 1000]}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"BucketizerConjure"}""")).
      conjure
  }
}
