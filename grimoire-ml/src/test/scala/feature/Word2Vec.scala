package feature

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
      master("local[4]").
      appName("VectorIndexer").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"Word2Vec2"}""").
      cast(DataFrameWord2VecSpell("""{"InputCol":"text","OutputCol":"result","vectorsize":3,"MinCount":1}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"Word2Vec2Conjure"}""")).
      conjure
  }
}