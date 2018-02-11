package feature
/**
  * Created by Aron on 17-6-28.
  */

import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameDCTSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object DCT {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("DCT").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"DCT"}""").
      cast(DataFrameDCTSpell("""{"InputCol":"features","OutputCol":"featuresDCT","Inverse":false}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"DCTConjure"}""")).
      conjure
  }
}
