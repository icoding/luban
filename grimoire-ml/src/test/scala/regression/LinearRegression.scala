package regression
/**
  * Created by Aron on 17-11-7.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.classify.transform.DataFrameLinearRegressionTrainSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.target.ModelTarget

object LinearRegression {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("LinearRegression").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"LinearRegression"}""").
      cast(DataFrameLinearRegressionTrainSpell("""{"LabelCol":"label","FeaturesCol":"features","PredictionCol":"prediction","MaxIter":10,"RegParam":0.3,"ElasticNetParam":0.8}""")).
      cast(ModelTarget("""{"OutputModelTarget":"hdfs:///user/model/LinearRegression","Overwrite":true}""")).
      conjure
  }
}
