package grimoire.ml.feature.conjure

/**
  * Created by Aron on 17-11-6.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameWord2VecSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object Word2Vec {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("Word2Vec").
      enableHiveSupport().
      getOrCreate()

    HiveSource(args(0)).
      cast(DataFrameWord2VecSpell(args(1))).
      cast(DataFrameTarget(args(2))).
      conjure
  }
}
