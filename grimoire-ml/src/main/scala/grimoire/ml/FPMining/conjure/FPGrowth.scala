package grimoire.ml.FPMining.conjure

/**
  * Created by Aron on 17-11-8.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import grimoire.ml.FPMining.FPGrowthDataFrameSpell
import grimoire.spark.target.DataFrameTarget
import grimoire.Implicits._


object FPGrowth {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("FPGrowth").
      enableHiveSupport().
      getOrCreate()

    HiveSource(args(0)).
      cast(FPGrowthDataFrameSpell(args(1))).
      cast(DataFrameTarget(args(2))).conjure

  }
}
