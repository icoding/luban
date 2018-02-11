package grimoire.ml.recommendation.conjure

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
      appName("ALSRecommend").
      enableHiveSupport().
      getOrCreate()

    HiveSource(args(0)).
      cast(ALSDataFrameSpell(args(1))).
      cast(DataFrameTarget(args(2))).conjure

  }
}
