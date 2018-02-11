package grimoire.ml.test.evaluate

import grimoire.ml.classify.transform.{DataFrameLBFGSPredictSpell, LogisticRegressionWithLBFGSTrainSpell}
import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell, DataFrameStringIndexerSpell, TakeDFSpell}
import grimoire.ml.transform.StringLabelIndexedSpell
import grimoire.spark.source.dataframe.CSVSource
import grimoire.spark.globalSpark
import grimoire.spark.transform.dataframe.DataFrameFilterSpell
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.evaluate.DataFrameBinaryClassificationEvaluatorSpell

/**
  * Created by sjc505 on 17-7-5.
  */
object BinaryClassificationEvaluationTest {
  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val data = CSVSource("""{"InputPath":"data/iris.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label string"}""").
    cast(DataFrameFilterSpell("""{}""").setFilterExpr("label <> 'setosa'"))

  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false)).cast(DataFrameStringIndexerSpell().setInputCol("label").setOutputCol("indexedlabel"))


  val labmap = train.cast(StringLabelIndexedSpell("""{}""").setInputCol("label"))

  val mod = (train :+ labmap).cast(LogisticRegressionWithLBFGSTrainSpell("""{}""").setNumClasses(10))

  val pre = (test :+ mod).cast(DataFrameLBFGSPredictSpell("""{"FeatureCols":["f1","f2","f3","f4"],"OutputCol":"pred","LabelCol":"labelCol"}"""))

  val ev = pre.cast(DataFrameBinaryClassificationEvaluatorSpell().setLabelCol("indexedlabel").setRawPredictionCol("pred").setMetricName("areaUnderROC"))

}
