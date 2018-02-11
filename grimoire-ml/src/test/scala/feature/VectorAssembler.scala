package feature
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.feature.transform.DataFrameVectorAssemblerSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

object VectorAssembler {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("VectorAssembler").
      enableHiveSupport().
      getOrCreate()

    HiveSource("""{"InputHiveTable":"VectorAssembler"}""").
      cast(DataFrameVectorAssemblerSpell("""{"InputCols":["hour", "mobile", "userFeatures"],"OutputCol":"features"}""")).
      cast(DataFrameTarget("""{"OutputHiveTable":"VectorAssemblerConjure"}""")).
      conjure
  }
}
