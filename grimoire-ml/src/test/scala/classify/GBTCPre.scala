package classify
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.classify.source.GDBCModelSource
import grimoire.ml.classify.transform.DataFrameGBTClassifierPredictSpell

object GBTCPre {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("GBTCPre").
      master("local[1]").
      enableHiveSupport().
      getOrCreate()

    (HiveSource("""{"InputHiveTable":"DecisionTree"}""") :+ GDBCModelSource("""{"InputModelSource":"hdfs:///user/model/GBTC"}""")).
      cast(DataFrameGBTClassifierPredictSpell()).
      cast(DataFrameTarget("""{"OutputHiveTable":"GBTCTestPre"}""")).conjure
  }
}