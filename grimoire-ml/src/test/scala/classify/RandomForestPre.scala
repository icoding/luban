package classify
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.classify.source.RandomForestModelSource
import grimoire.ml.classify.transform.DataFrameRandomForestClassifierPredictSpell

object RandomForestPre {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("RandomForestPre").
      master("local[1]").
      enableHiveSupport().
      getOrCreate()

    (HiveSource("""{"InputHiveTable":"DecisionTree"}""") :+ RandomForestModelSource("""{"InputModelSource":"hdfs:///user/model/RandomForest"}""")).
      cast(DataFrameRandomForestClassifierPredictSpell()).
      cast(DataFrameTarget("""{"OutputHiveTable":"RandomForestTestPre"}""")).conjure
  }
}
