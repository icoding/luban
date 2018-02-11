package grimoire.ml.evaluate.conjure
/**
  * Created by Aron on 17-11-20.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.{HiveSource, JsonSource}
import org.apache.spark.sql.SparkSession
import grimoire.ml.classify.source._
import grimoire.ml.classify.transform._
import grimoire.ml.evaluate.DataFrameRegressionMetricsSpell
import grimoire.Implicits._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.PrintWriter
import grimoire.spark.target.DataFrameTarget


object RegressionMetrics {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      appName("RegressionMetrics").
      enableHiveSupport().
      getOrCreate()


    val metrics = HiveSource(args(0)).
      cast(DataFrameRegressionMetricsSpell(args(1))).conjure

    val conf = new Configuration()
    val fs= FileSystem.get(conf)
    val output = fs.create(new Path("hdfs:///user/RegressionMetrics"))
    val writer = new PrintWriter(output)
    try{
      writer.write(metrics.toString())
      writer.write("\n")
    }finally {
      writer.close()
    }
    JsonSource().setInputPath("hdfs:///user/RegressionMetrics").
      cast(DataFrameTarget(args(2))).conjure
  }
}