package regression
/**
  * Created by Aron on 17-11-7.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.classify.transform.DataFrameDecisionTreeRegressorTrainSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.target.ModelTarget

object DecisionTreeRegression {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("DecisionTreeRegression").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"DecisionTreeRegression"}""").
      cast(DataFrameDecisionTreeRegressorTrainSpell("""{"LabelCol":"label","FeaturesCol":"features","PredictionCol":"prediction"}""")).
      cast(ModelTarget("""{"OutputModelTarget":"hdfs:///user/model/DecisionTreeRegression","Overwrite":true}""")).
      conjure
  }

}
