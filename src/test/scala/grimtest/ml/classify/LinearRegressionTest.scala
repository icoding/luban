package grimtest.ml.classify

import grimoire.ml.classify.transform.{DataFrameLinearRegressionPredictSpell, DataFrameLinearRegressionTrainSpell}
import grimoire.ml.evaluate.DataFrameRegressionEvaluatorSpell
import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell, TakeDFSpell}
import grimoire.Implicits._
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.spark.source.dataframe.LibSvmSource

/**
  * Created by jax on 17-6-27.
  */
object LinearRegressionTest {


  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val data = LibSvmSource("""{"InputPath":"data/mllib/sample_linear_regression_data.txt"}""")
  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false))

  val mod = train.cast(DataFrameLinearRegressionTrainSpell().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).
      setLabelCol("label").setFeaturesCol("features"))


  val model = mod.conjure

  // Print the coefficients and intercept for linear regression
  println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

  // Summarize the model over the training set and print out some metrics
  val trainingSummary = model.summary
  println(s"numIterations: ${trainingSummary.totalIterations}")
  println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
  trainingSummary.residuals.show()
  println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
  println(s"r2: ${trainingSummary.r2}")

  // Make predictions.
  val predictions = (test:+mod).cast(DataFrameLinearRegressionPredictSpell())
  val rmse = predictions.cast(DataFrameRegressionEvaluatorSpell().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse"))
  println("Root Mean Squared Error (RMSE) on test data = " + rmse.conjure)
}
