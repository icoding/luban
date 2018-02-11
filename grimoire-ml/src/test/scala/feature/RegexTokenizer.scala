package feature
/**
  * Created by Aron on 17-6-28.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameRegexTokenizerSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object RegexTokenizer {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("RegexTokenizer").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"RegexTokenizer"}""").
      cast(DataFrameRegexTokenizerSpell("""{"InputCol":"sentence","OutputCol":"words","Pattern":"\\W"}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"RegexTokenizerConjure"}""")).
      conjure
  }
}
