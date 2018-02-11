package ml.test.classify


/**
  * Created by Aron on 17-6-28.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.{DataFrameCountVectorzerSpell, DataFrameRegexTokenizerSpell, DataFrameTFVectorizeSpell, DataFrameVectorAssemblerSpell}
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object countvectorizer_test {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("CountVectorzer").
      enableHiveSupport().
      getOrCreate()


    HiveSource("""{"InputHiveTable":"RegexTokenizer"}""").
      cast(DataFrameRegexTokenizerSpell("""{"InputCol":"sentence","OutputCol":"words","Pattern":"\\W"}""")).
      cast(DataFrameCountVectorzerSpell("""{"InputCol":"words","OutputCol":"output","VocabSize":30,"MinDF":2}""")).
      conjure

  }

}
