package regression
/**
  * Created by Aron on 17-11-6.
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
      master("local[1]").
      enableHiveSupport().
      getOrCreate()

    (HiveSource("""{"InputHiveTable":"DecisionTreeRegression"}""") :+ DecisionTreeRegressionModelSource("""{"InputModelSource":"hdfs:///user/model/DecisionTreeRegression"}""")).
      cast(DataFrameDecisionTreeRegressorPredictSpell()).
      cast(DataFrameTarget("""{"OutputHiveTable":"DecisionTreeRegressionPre"}""")).conjure
  }
}
