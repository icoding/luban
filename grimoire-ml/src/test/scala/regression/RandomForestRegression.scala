package regression
/**
  * Created by Aron on 17-11-8.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.classify.transform.DataFrameRandomForestRegressorTrainSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.target.ModelTarget

object RandomForestRegression {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("RandomForestRegression").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"DecisionTreeRegression"}""").
      cast(DataFrameRandomForestRegressorTrainSpell("""{"LabelCol":"label","FeaturesCol":"features","PredictionCol":"prediction","NumTrees":20}""")).
      cast(ModelTarget("""{"OutputModelTarget":"hdfs:///user/model/RandomForestRegression","Overwrite":true}""")).
      conjure
  }
}
