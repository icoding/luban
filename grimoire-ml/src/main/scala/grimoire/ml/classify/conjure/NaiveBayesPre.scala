package grimoire.ml.classify.conjure
/**
  * Created by Aron on 17-11-20.
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
      enableHiveSupport().
      getOrCreate()

    (HiveSource(args(0)) :+ NaiveBayesModelSource(args(1))).
      cast(DataFrameNaiveBayesPredictSpell()).
      cast(DataFrameTarget(args(2))).conjure
  }
}
