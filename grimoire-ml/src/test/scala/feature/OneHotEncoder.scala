package feature
/**
  * Created by Aron on 17-6-28.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameOneHotEncoderSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object OneHotEncoder {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("OneHotEncoder").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"OneHotEncoder"}""").
      cast(DataFrameOneHotEncoderSpell("""{"InputCol":"categoryIndex","OutputCol":"categoryVec","DropLast":false}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"OneHotEncoderConjure"}""")).
      conjure
  }
}
