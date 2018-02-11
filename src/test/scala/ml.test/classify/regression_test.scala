package ml.test.classify

import grimoire.ml.classify.transform.{DataFrameDecisionTreeRegressorPredictSpell, DataFrameDecisionTreeRegressorTrainSpell}
import grimoire.ml.evaluate.{DataFrameRegressionEvaluatorSpell, DataFrameRegressionMetricsSpell}
import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell, DataFrameVectorAssemblerSpell, DataFrameVectorIndexerSpell, TakeDFSpell}
import grimoire.Implicits._
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.spark.source.dataframe.HiveSource

object regression_test {
  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val df = HiveSource("""{"InputHiveTable":"regression_test"}""").
    cast(DataFrameVectorAssemblerSpell("""{"InputCols":["f1", "f2", "f3","f4","f5","f6","f7","f8","f9","f10"],"OutputCol":"features"}"""))
  val train = df.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())

  val test = df.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false))

  val mod = df.
    cast(DataFrameDecisionTreeRegressorTrainSpell().setLabelCol("label").setFeaturesCol("features").setPredictionCol("prediction"))

  val pre = (test :+ mod).cast(DataFrameDecisionTreeRegressorPredictSpell())

  val ev = pre.cast(DataFrameRegressionMetricsSpell("""{"LabelCol":"label","PredictionCol":"prediction"}"""))

}
