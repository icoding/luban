package statistics
/**
  * Created by Aron on 17-11-16.
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

object Correlations {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("Correlations").
      enableHiveSupport().
      getOrCreate()

    val jsonObject = HiveSource("""{"InputHiveTable":"iris"}""").
      cast(DataFrameCorrelationsSpell("""{"InputCols":["f1","f2","f3","f4"],"CorrelationMethod":"pearson"}""")).conjure

    val conf = new Configuration()
    val fs= FileSystem.get(conf)
//    val output = fs.create(new Path(JsonReaders.stringReader.read("""{"OutPutJsonTarget":"hdfs:///user/example/jsonss"}""","OutPutJsonTarget")))
    val output = fs.create(new Path("hdfs:///user/Correlations"))
    val writer = new PrintWriter(output)
    try{
      writer.write(jsonObject.toString())
      writer.write("\n")
    }finally {
      writer.close()
    }
    JsonSource().setInputPath("hdfs:///user/Correlations").
      cast(DataFrameTarget("""{"OutputHiveTable":"Correlations_demo_conjure"}""")).conjure
  }
}