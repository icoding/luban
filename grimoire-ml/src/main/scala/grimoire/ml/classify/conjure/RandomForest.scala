package grimoire.ml.classify.conjure
/**
  * Created by Aron on 17-11-20.
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
      appName("RandomForest").
      enableHiveSupport().
      getOrCreate()

    HiveSource(args(0)).
      cast(DataFrameRandomForestClassifierTrainSpell(args(1))).
      cast(ModelTarget(args(2))).
      conjure
  }
}
