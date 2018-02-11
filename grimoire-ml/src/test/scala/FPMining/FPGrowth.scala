package FPMining

import grimoire.Implicits.jstr2JsValue
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import grimoire.ml.FPMining.FPGrowthDataFrameSpell
import grimoire.spark.target.DataFrameTarget

object FPGrowth {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("FPGrowth").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"huarun_shop_rel"}""").
      cast(FPGrowthDataFrameSpell("""{"minSupport":0.02,"FeaturesCol":"shopId","minConfidence":0.2}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"huarun_shop_rel_conjure"}""")).conjure
  }
}
