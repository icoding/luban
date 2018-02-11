package grimtest.ml.classify

import grimoire.Implicits._
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.ml.classify.transform.{DataFrameIsotonicRegressionPredictSpell, DataFrameIsotonicRegressionTrainSpell, DataFrameMulticlassClassificationEvaluatorSpell}
import grimoire.ml.evaluate.DataFrameRegressionEvaluatorSpell
import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell, TakeDFSpell}
import grimoire.spark.source.dataframe.LibSvmSource
/**
  * Created by jax on 17-6-28.
  */
object IsotonicRegressionTest {


  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val data = LibSvmSource("""{"InputPath":"data/mllib/sample_isotonic_regression_libsvm_data.txt"}""")
  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false))

  val mod = train.cast(DataFrameIsotonicRegressionTrainSpell())

  val pre = (test :+ mod).cast(DataFrameIsotonicRegressionPredictSpell())

  val ev = pre.cast(DataFrameRegressionEvaluatorSpell().setPredictionCol("prediction").setMetricName("rmse"))


  //  val model = mod.conjure
//
//
//  println(s"Boundaries in increasing order: ${model.boundaries}\n")
//  println(s"Predictions associated with the boundaries: ${model.predictions}\n")
}
