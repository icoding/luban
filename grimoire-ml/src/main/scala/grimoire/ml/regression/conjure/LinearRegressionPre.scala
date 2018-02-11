package grimoire.ml.regression.conjure
/**
  * Created by Aron on 17-11-20.
  */
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.classify.source.LinearRegressionModelSource
import grimoire.ml.classify.transform.DataFrameLinearRegressionPredictSpell

object LinearRegressionPre {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("LinearRegressionPre").
      enableHiveSupport().
      getOrCreate()

    (HiveSource(args(0)) :+ LinearRegressionModelSource(args(1))).
      cast(DataFrameLinearRegressionPredictSpell()).
      cast(DataFrameTarget(args(2))).conjure
  }
}
