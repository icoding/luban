package feature
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameIndexToStringSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object IndexToString {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("IndexToString").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"IndexToString"}""").
      cast(DataFrameIndexToStringSpell("""{"InputCol":"categoryIndex","OutputCol":"originalCategory"}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"IndexToStringConjure"}""")).
      conjure
  }
}
