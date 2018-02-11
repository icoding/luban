package feature
/**
  * Created by Aron on 17-6-28.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameCountVectorzerSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object CountVectorzer {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("CountVectorzer").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"countvectorizer"}""").
      cast(DataFrameCountVectorzerSpell("""{"InputCol":"words","OutputCol":"features","VocabSize":3,"MinDF":2}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"CountVectorzerConjure"}""")).
      conjure
  }

}
