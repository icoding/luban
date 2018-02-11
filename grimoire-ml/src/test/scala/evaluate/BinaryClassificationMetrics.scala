package evaluate
/**
  * Created by Aron on 17-11-14.
  */
import grimoire.Implicits.jstr2JsValue
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.{HiveSource, JsonSource}
import org.apache.spark.sql.SparkSession
import grimoire.ml.classify.source._
import grimoire.ml.classify.transform._
import grimoire.ml.evaluate.DataFrameBinaryClassificationMetricsSpell
import grimoire.ml.exception.ModelSourceException
import grimoire.Implicits._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.PrintWriter
import grimoire.spark.target.DataFrameTarget
import grimoire.util.json.reader.JsonReaders


object BinaryClassificationMetrics {
  def main(args: Array[String]): Unit = {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("Binarizer").
      enableHiveSupport().
      getOrCreate()

    val metrics = "DecisionTree" match {
      case "DecisionTree" => {
        (HiveSource("""{"InputHiveTable":"DecisionTree"}""") :+ DecisionTreeModelSource("""{"InputModelSource":"hdfs:///user/model/DecisionTree"}""")).
          cast(DataFrameDecisionTreeClassifierPredictSpell()).
          cast(DataFrameBinaryClassificationMetricsSpell("""{"LabelCol":"label","PredictionCol":"prediction"}""")).conjure
      }
      case "LogisticRegression" => {
        (HiveSource("""{"InputHiveTable":"LogisticRegression"}""") :+ LogisticRegressionModelSource("""{"InputModelSource":"hdfs:///user/model/LogisticRegression"}""")).
          cast(DataFrameLogisticRegressionPredictSpell()).
          cast(DataFrameBinaryClassificationMetricsSpell("""{"LabelCol":"label","PredictionCol":"predic"}""")).conjure
      }
      case "RandomForest" => {
        (HiveSource("""{"InputHiveTable":"DecisionTree"}""") :+ RandomForestModelSource("""{"InputModelSource":"hdfs:///user/model/RandomForest"}""")).
          cast(DataFrameRandomForestClassifierPredictSpell()).
          cast(DataFrameBinaryClassificationMetricsSpell("""{"LabelCol":"label","PredictionCol":"prediction"}""")).conjure
      }
      case "GBTC" => {
        (HiveSource("""{"InputHiveTable":"DecisionTree"}""") :+ GDBCModelSource("""{"InputModelSource":"hdfs:///user/model/GBTC"}""")).
          cast(DataFrameGBTClassifierPredictSpell()).
          cast(DataFrameBinaryClassificationMetricsSpell("""{"LabelCol":"label","PredictionCol":"prediction"}""")).conjure
      }
      case "NaiveBayes" => {
        (HiveSource("""{"InputHiveTable":"DecisionTree"}""") :+ NaiveBayesModelSource("""{"InputModelSource":"hdfs:///user/model/NaiveBayes"}""")).
          cast(DataFrameNaiveBayesPredictSpell()).
          cast(DataFrameBinaryClassificationMetricsSpell("""{"LabelCol":"label","PredictionCol":"prediction"}""")).conjure
      }
      case _ => throw ModelSourceException("test")
    }

    val conf = new Configuration()
    val fs= FileSystem.get(conf)
    val output = fs.create(new Path(JsonReaders.stringReader.read("""{"OutPutJsonTarget":"hdfs:///user/example/jsonss.json"}""","OutPutJsonTarget")))
    val writer = new PrintWriter(output)
    try{
      writer.write(metrics.toString())
      writer.write("\n")
    }finally {
      writer.close()
    }

    JsonSource().setInputPath("hdfs:///user/example/jsonss.json").
      cast(DataFrameTarget("""{"OutputHiveTable":"BinaryClassificationMetricsDataframe"}""")).conjure
  }
}
