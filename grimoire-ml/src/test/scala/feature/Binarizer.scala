package feature
/**
  * Created by Aron on 17-6-28.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameBinarizerSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object Binarizer {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("Binarizer").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"Binarizer"}""").
      cast(DataFrameBinarizerSpell("""{"InputCol":"feature","OutputCol":"binarized_feature","Threshold":0.5}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"BinarizerConjure"}""")).
      conjure
  }
}
