package classify
/**
  * Created by Aron on 17-11-7.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.classify.transform.DataFrameNaiveBayesTrainSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.target.ModelTarget

object NaiveBayes {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("NaiveBayes").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"DecisionTree"}""").
      cast(DataFrameNaiveBayesTrainSpell("""{"LabelCol":"label","FeaturesCol":"features","PredictionCol":"prediction"}""")).
      cast(ModelTarget("""{"OutputModelTarget":"hdfs:///user/model/NaiveBayes","Overwrite":true}""")).
      conjure
  }
}

