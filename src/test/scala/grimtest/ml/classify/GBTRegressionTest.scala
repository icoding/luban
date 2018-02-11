package grimtest.ml.classify

import grimoire.ml.classify.transform.{DataFrameGBTRegressorPredictSpell, DataFrameGBTRegressorTrainSpell}
import grimoire.ml.evaluate.DataFrameRegressionEvaluatorSpell
import grimoire.Implicits._
import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell, DataFrameVectorIndexerSpell, TakeDFSpell}
import grimoire.spark.source.dataframe.LibSvmSource
import grimoire.spark.globalSpark
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.spark.sql.SparkSession

/**
  * Created by jax on 17-6-28.
  */
object GBTRegressionTest {


  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val data = LibSvmSource("""{"InputPath":"data/mllib/sample_libsvm_data.txt"}""").
    cast(DataFrameVectorIndexerSpell().setInputCol("features").setOutputCol("indexedfeatures").setMaxCategories(4))
  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false))

  val mod = train.cast(DataFrameGBTRegressorTrainSpell().setLabelCol("label").setFeaturesCol("indexedfeatures").setMaxIter(10))

  val pre = (test :+ mod).cast(DataFrameGBTRegressorPredictSpell())

  val ev = pre.cast(DataFrameRegressionEvaluatorSpell().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse"))



  //  val model = mod.conjure
//
//  // Make predictions.
//
//  val predictions = model.transform(test.conjure)
//
//  // Select example rows to display.
//  predictions.select("prediction", "label", "features").show(5)
//
//  // Select (prediction, true label) and compute test error.
//  val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")
//  val rmse = evaluator.evaluate(predictions)
//  println("Root Mean Squared Error (RMSE) on test data = " + rmse)
//
//
//  val treeModel = model.asInstanceOf[GBTRegressionModel]
//  println("Learned regression tree model:\n" + treeModel.toDebugString)
}
