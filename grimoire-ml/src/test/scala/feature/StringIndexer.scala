package feature

import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameStringIndexerSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
/**
  * Created by Aron on 17-6-28.
  */


object StringIndexer {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("StringIndexer").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"StringIndexer"}""").
      cast(DataFrameStringIndexerSpell("""{"InputCol":"category","OutputCol":"categoryIndex"}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"StringIndexerConjure"}""")).
      conjure
  }
}
