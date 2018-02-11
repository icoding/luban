package feature
/**
  * Created by Aron on 17-6-28.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameNGramSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object NGram {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("NGram").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"NGram"}""").
      cast(DataFrameNGramSpell("""{"InputCol":"words","OutputCol":"ngrams","N":2}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"NGramConjure"}""")).
      conjure
  }
}
