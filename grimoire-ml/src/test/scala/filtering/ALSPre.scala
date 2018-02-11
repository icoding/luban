package filtering
/**
  * Created by Aron on 17-11-6.
  */
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import grimoire.spark.target.DataFrameTarget
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.filtering.source.ALSModelSource
import grimoire.ml.filtering.transform.DataFrameALSPredictSpell

object ALSPre {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("ALSPre").
      master("local[1]").
      enableHiveSupport().
      getOrCreate()

    (HiveSource("""{"InputHiveTable":"ALS"}""") :+ ALSModelSource("""{"InputModelSource":"hdfs:///user/model/ALS"}""")).
      cast(DataFrameALSPredictSpell()).
      cast(DataFrameTarget("""{"OutputHiveTable":"ALSPre"}""")).conjure
  }
}

