package feature
/**
  * Created by Aron on 17-6-28.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameTFVectorizeSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object TFVectorize {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local").
      appName("test").
      enableHiveSupport().
      config("spark.sql.warehouse.dir", "/home/wuxiang/hive-1.2.2/metastore_db").
      getOrCreate()

    HiveSource("""{"InputHiveTable":"wfwfwfwf"}""").
      cast(DataFrameTFVectorizeSpell("""{"InputCol":"segmented","OutputCol":"tf","NumFeatures":100,"Binary":true}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"test2"}""")).
      conjure
  }
}
