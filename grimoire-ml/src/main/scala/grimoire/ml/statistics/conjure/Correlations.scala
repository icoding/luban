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

import grimoire.ml.statistics.DataFrameCorrelationsSpell
import grimoire.spark.target.DataFrameTarget
import grimoire.util.json.reader.JsonReaders
import play.api.libs.json._

object Correlations {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("Correlations").
      enableHiveSupport().
      getOrCreate()

    val jsonObject = HiveSource(args(0)).
      cast(DataFrameCorrelationsSpell(
        Json.obj(
          "InputCols"-> (Json.parse(args(1)) \ "InputCols").as[String].split(",").map(_.trim),
          "CorrelationMethod" -> (Json.parse(args(1)) \ "CorrelationMethod").as[String]
        ).toString()
      )).conjure

    val conf = new Configuration()
    val fs= FileSystem.get(conf)
    val output = fs.create(new Path("hdfs:///user/Correlations"))
    val writer = new PrintWriter(output)
    try{
      writer.write(jsonObject.toString())
      writer.write("\n")
    }finally {
      writer.close()
    }

    JsonSource().setInputPath("hdfs:///user/Correlations").
      cast(DataFrameTarget(args(2))).conjure
  }
}