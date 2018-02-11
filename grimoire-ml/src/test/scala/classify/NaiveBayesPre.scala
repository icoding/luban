package classify
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.classify.source.NaiveBayesModelSource
import grimoire.ml.classify.transform.DataFrameNaiveBayesPredictSpell

object NaiveBayesPre {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("RandomForestPre").
      master("local[1]").
      enableHiveSupport().
      getOrCreate()

    (HiveSource("""{"InputHiveTable":"DecisionTree"}""") :+ NaiveBayesModelSource("""{"InputModelSource":"hdfs:///user/model/NaiveBayes"}""")).
      cast(DataFrameNaiveBayesPredictSpell()).
      cast(DataFrameTarget("""{"OutputHiveTable":"NaiveBayesTestPre"}""")).conjure
  }
}
