package statistics
/**
  * Created by Aron on 17-11-15.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.PrintWriter

import grimoire.ml.statistics.DataFrameMultivariateStatisticalSummarySpell
import grimoire.util.json.reader.JsonReaders

object StatisticalSummary {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("StatisticalSummary").
      enableHiveSupport().
      getOrCreate()

    val jsonObject = HiveSource("""{"InputHiveTable":"iris"}""").
    cast(DataFrameMultivariateStatisticalSummarySpell("""{"InputCols":["f1","f2","f3","f4"],"ColLabels":["f1","f2","f3","f4"]}""")).conjure

    val conf = new Configuration()
    val fs= FileSystem.get(conf)
    val output = fs.create(new Path(JsonReaders.stringReader.read("""{"OutPutJsonTarget":"hdfs:///user/example/jsonss"}""","OutPutJsonTarget")))
    val writer = new PrintWriter(output)
    try{
      writer.write(jsonObject.toString())
      writer.write("\n")
    }finally {
      writer.close()
    }
  }
}