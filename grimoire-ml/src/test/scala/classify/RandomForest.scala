package classify
/**
  * Created by Aron on 17-11-7.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.classify.transform.DataFrameRandomForestClassifierTrainSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.target.ModelTarget

object RandomForest {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("RandomForest").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"DecisionTree"}""").
      cast(DataFrameRandomForestClassifierTrainSpell("""{"LabelCol":"label","FeaturesCol":"features","PredictionCol":"prediction","NumTrees":10}""")).
      cast(ModelTarget("""{"OutputModelTarget":"hdfs:///user/model/RandomForest","Overwrite":true}""")).
      conjure
  }
}
