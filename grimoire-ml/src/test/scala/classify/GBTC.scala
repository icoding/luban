package classify
/**
  * Created by Aron on 17-11-7.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.classify.transform.DataFrameGBTClassifierTrainSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.target.ModelTarget

object GBTC {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("DecisionTree").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"DecisionTree"}""").
      cast(DataFrameGBTClassifierTrainSpell("""{"LabelCol":"label","FeaturesCol":"features","PredictionCol":"prediction","MaxIter":10}""")).
      cast(ModelTarget("""{"OutputModelTarget":"hdfs:///user/model/GBTC","Overwrite":true}""")).
      conjure
  }
}
