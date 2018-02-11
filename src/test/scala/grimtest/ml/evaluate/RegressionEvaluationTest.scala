package grimoire.ml.test.evaluate

import grimoire.ml.classify.transform.{DataFrameGBTRegressorPredictSpell, DataFrameGBTRegressorTrainSpell}
import grimoire.ml.evaluate.DataFrameRegressionEvaluatorSpell
import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell, DataFrameVectorIndexerSpell, TakeDFSpell}
import grimoire.spark.source.dataframe.LibSvmSource
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-5.
  */
object RegressionEvaluationTest {

  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val data = LibSvmSource("""{"InputPath":"data/mllib/sample_libsvm_data.txt"}""").
    cast(DataFrameVectorIndexerSpell().setInputCol("features").setOutputCol("indexedfeatures").setMaxCategories(4))
  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false))

  val mod = train.cast(DataFrameGBTRegressorTrainSpell().setLabelCol("label").setFeaturesCol("indexedfeatures").setMaxIter(10))

  val pre = (test :+ mod).cast(DataFrameGBTRegressorPredictSpell())

  val ev = pre.cast(DataFrameRegressionEvaluatorSpell().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse"))

}
