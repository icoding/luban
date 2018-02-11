package classify
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.classify.source.LogisticRegressionModelSource
import grimoire.ml.classify.transform.DataFrameLogisticRegressionPredictSpell

object LogisticRegressionPre {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("LogisticRegressionPre").
      master("local[1]").
      enableHiveSupport().
      getOrCreate()

    (HiveSource("""{"InputHiveTable":"LogisticRegression"}""") :+ LogisticRegressionModelSource("""{"InputModelSource":"hdfs:///user/model/LogisticRegression"}""")).
      cast(DataFrameLogisticRegressionPredictSpell()).
      cast(DataFrameTarget("""{"OutputHiveTable":"LogisticRegressionTestPre"}""")).conjure
  }
}
