package feature
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameChiSqSelectorSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object ChiSqSelector {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("ChiSqSelector").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"ChiSqSelector"}""").
      cast(DataFrameChiSqSelectorSpell("""{"NumTopFeatures":1,"FeaturesCol":"vec","LabelCol":"label","OutputCol":"chisq"}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"ChiSqSelectorConjure"}""")).
      conjure
  }
}
