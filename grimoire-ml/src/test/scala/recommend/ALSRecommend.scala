package recommend

import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.recommendation.transform.ALSDataFrameSpell

object ALSRecommend {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local").
      appName("ALSRecommend").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"huarun_als_price"}""").
      cast(ALSDataFrameSpell("""{"Rank":10,"NumIterations":10,"Lambda":0.01,"NumRecommend":300}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"huarun_als_price_conjure"}""")).conjure
  }
}