package classify
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.classify.transform.{DataFrameLogisticRegressionPredictSpell, DataFrameLogisticRegressionTrainSpell}
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.target.ModelTarget

object LogisticRegression {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("LogisticRegression").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"LogisticRegression"}""").
      cast(DataFrameLogisticRegressionTrainSpell("""{"MaxIter":10,"RegParam":0.3,"ElasticNetParam":0.8,"LabelCol":"label","FeaturesCol":"features","PredictionCol":"predic"}""")).
    cast(ModelTarget("""{"OutputModelTarget":"hdfs:///user/model/LogisticRegression","Overwrite":true}""")).
      conjure
  }
}
