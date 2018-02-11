package grimoire.ml.statistics.conjure
/**
  * Created by Aron on 17-11-20.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.{HiveSource, JsonSource}
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.PrintWriter

import grimoire.ml.statistics.DataFrameMultivariateStatisticalSummarySpell
import grimoire.spark.target.DataFrameTarget
import play.api.libs.json._

object StatisticalSummary {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("StatisticalSummary").
      enableHiveSupport().
      getOrCreate()

    val jsonObject = HiveSource(args(0)).
      cast(DataFrameMultivariateStatisticalSummarySpell(
        Json.obj(
          "InputCols"-> (Json.parse(args(1)) \ "InputCols").as[String].split(",").map(_.trim),
          "ColLabels" -> (Json.parse(args(1)) \ "InputCols").as[String].split(",").map(_.trim)
        ).toString()
      )).conjure

    val conf = new Configuration()
    val fs= FileSystem.get(conf)
    val output = fs.create(new Path("hdfs:///user/StatisticalSummary"))
    val writer = new PrintWriter(output)
    try{
      writer.write(jsonObject.toString())
      writer.write("\n")
    }finally {
      writer.close()
    }

    JsonSource().setInputPath("hdfs:///user/StatisticalSummary").
      cast(DataFrameTarget(args(2))).conjure
  }
}