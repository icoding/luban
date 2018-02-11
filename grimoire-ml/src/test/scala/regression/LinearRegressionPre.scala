package regression
/**
  * Created by Aron on 17-11-6.
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
      master("local[1]").
      enableHiveSupport().
      getOrCreate()

    (HiveSource("""{"InputHiveTable":"LinearRegression"}""") :+ LinearRegressionModelSource("""{"InputModelSource":"hdfs:///user/model/LinearRegression"}""")).
      cast(DataFrameLinearRegressionPredictSpell()).
      cast(DataFrameTarget("""{"OutputHiveTable":"LinearRegressionPre"}""")).conjure
  }
}
