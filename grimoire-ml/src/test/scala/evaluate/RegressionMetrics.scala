package evaluate
/**
  * Created by Aron on 17-11-14.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.HiveSource
import org.apache.spark.sql.SparkSession
import grimoire.ml.classify.source._
import grimoire.ml.classify.transform._
import grimoire.ml.evaluate.{DataFrameBinaryClassificationMetricsSpell, DataFrameRegressionMetricsSpell}
import grimoire.ml.exception.ModelSourceException
import grimoire.Implicits._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.PrintWriter

import grimoire.util.json.reader.JsonReaders

object RegressionMetrics {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("Binarizer").
      enableHiveSupport().
      getOrCreate()

    val metrics = "DecisionTreeRegression" match {
      case "DecisionTreeRegression" => {
        (HiveSource("""{"InputHiveTable":"DecisionTreeRegression"}""") :+ DecisionTreeRegressionModelSource("""{"InputModelSource":"hdfs:///user/model/DecisionTreeRegression"}""")).
          cast(DataFrameDecisionTreeRegressorPredictSpell()).
          cast(DataFrameRegressionMetricsSpell("""{"LabelCol":"label","PredictionCol":"prediction"}""")).conjure
      }
      case "LinearRegression" => {
        (HiveSource("""{"InputHiveTable":"LinearRegression"}""") :+ LinearRegressionModelSource("""{"InputModelSource":"hdfs:///user/model/LinearRegression"}""")).
          cast(DataFrameLinearRegressionPredictSpell()).
          cast(DataFrameRegressionMetricsSpell("""{"LabelCol":"label","PredictionCol":"prediction"}""")).conjure
      }
      case "RandomForestRegression" => {
        (HiveSource("""{"InputHiveTable":"DecisionTreeRegression"}""") :+ RandomForestRegressionModelSource("""{"InputModelSource":"hdfs:///user/model/RandomForestRegression"}""")).
          cast(DataFrameRandomForestRegressorPredictSpell()).
          cast(DataFrameRegressionMetricsSpell("""{"LabelCol":"label","PredictionCol":"prediction"}""")).conjure
      }
    }

    val conf = new Configuration()
    val fs= FileSystem.get(conf)
    val output = fs.create(new Path(JsonReaders.stringReader.read("""{"OutPutJsonTarget":"hdfs:///user/example/jsonss"}""","OutPutJsonTarget")))
    val writer = new PrintWriter(output)
    try{
      writer.write(metrics.toString())
      writer.write("\n")
    }finally {
      writer.close()
    }
  }
}