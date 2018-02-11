package grimoire.ml.regression.conjure
/**
  * Created by Aron on 17-11-20.
  */
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.classify.source.DecisionTreeRegressionModelSource
import grimoire.ml.classify.transform.DataFrameDecisionTreeRegressorPredictSpell

object DecisionTreeRegressionPre {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("DecisionTreeRegressionPre").
      enableHiveSupport().
      getOrCreate()

    (HiveSource(args(0)) :+ DecisionTreeRegressionModelSource(args(1))).
      cast(DataFrameDecisionTreeRegressorPredictSpell()).
      cast(DataFrameTarget(args(2))).conjure
  }
}
