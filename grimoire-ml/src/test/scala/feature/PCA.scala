package feature
/**
  * Created by Aron on 17-6-28.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFramePCASpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object PCA {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("PCA").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"PCA"}""").
      cast(DataFramePCASpell("""{"InputCol":"features","OutputCol":"pcaFeatures","K":3}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"PCAConjure"}""")).
      conjure
  }
}
