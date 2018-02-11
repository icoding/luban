package grimtest.ml.classify

import grimoire.ml.classify.transform.{DataFrameDecisionTreeRegressorPredictSpell, DataFrameDecisionTreeRegressorTrainSpell}
import grimoire.ml.evaluate.DataFrameRegressionEvaluatorSpell
import grimoire.ml.feature.transform.DataFrameVectorIndexerSpell
import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell,TakeDFSpell}
import grimoire.Implicits._
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.spark.source.dataframe.LibSvmSource


/**
  * Created by jax on 17-6-27.
  */
object DecisionTreeRegressorTest {

  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val data = LibSvmSource("""{"InputPath":"data/mllib/sample_libsvm_data.txt"}""").
    cast(DataFrameVectorIndexerSpell().setInputCol("features").setOutputCol("indexedfeatures").setMaxCategories(4))
  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false))

  val mod = train.cast(DataFrameDecisionTreeRegressorTrainSpell().setLabelCol("label").setFeaturesCol("indexedfeatures"))

  val pre = (test :+ mod).cast(DataFrameDecisionTreeRegressorPredictSpell())

  val ev = pre.cast(DataFrameRegressionEvaluatorSpell().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse"))



}
