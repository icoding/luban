package grimoire.ml.regression.conjure

/**
  * Created by Aron on 17-11-8.
  */
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.classify.source.RandomForestRegressionModelSource
import grimoire.ml.classify.transform.DataFrameRandomForestRegressorPredictSpell

object RandomForestRegressionPre {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("RandomForestRegressionPre").
      enableHiveSupport().
      getOrCreate()

    (HiveSource(args(0)) :+ RandomForestRegressionModelSource(args(1))).
      cast(DataFrameRandomForestRegressorPredictSpell()).
      cast(DataFrameTarget(args(2))).conjure
  }
}

