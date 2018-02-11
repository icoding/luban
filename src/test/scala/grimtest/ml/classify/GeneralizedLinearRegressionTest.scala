package grimtest.ml.classify

import grimoire.ml.classify.transform.{DataFrameGeneralizedLinearRegressionPredictSpell, DataFrameGeneralizedLinearRegressionTrainSpell}
import grimoire.ml.evaluate.DataFrameRegressionEvaluatorSpell
import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell, TakeDFSpell}
import grimoire.Implicits._
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.spark.source.dataframe.LibSvmSource

/**
  * Created by jax on 17-6-27.
  */
object GeneralizedLinearRegressionTest {


  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files", "conf/hive-site.xml").getOrCreate()
  val data = LibSvmSource("""{"InputPath":"data/mllib/sample_linear_regression_data.txt"}""")
  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false))

  val mod = train.
    cast(DataFrameGeneralizedLinearRegressionTrainSpell().setFamily("gaussian").setLink("identity").setMaxIter(10).setRegParam(0.3).setLabelCol("label").setFeaturesCol("features"))

  val model = mod.conjure
  // Print the coefficients and intercept for generalized linear regression model
  println(s"Coefficients: ${model.coefficients}")
  println(s"Intercept: ${model.intercept}")

  // Summarize the model over the training set and print out some metrics
  val summary = model.summary
  println(s"Coefficient Standard Errors: ${summary.coefficientStandardErrors.mkString(",")}")
  println(s"T Values: ${summary.tValues.mkString(",")}")
  println(s"P Values: ${summary.pValues.mkString(",")}")
  println(s"Dispersion: ${summary.dispersion}")
  println(s"Null Deviance: ${summary.nullDeviance}")
  println(s"Residual Degree Of Freedom Null: ${summary.residualDegreeOfFreedomNull}")
  println(s"Deviance: ${summary.deviance}")
  println(s"Residual Degree Of Freedom: ${summary.residualDegreeOfFreedom}")
  println(s"AIC: ${summary.aic}")
  println("Deviance Residuals: ")
  summary.residuals().show()


  val pre = (test :+ mod).cast(DataFrameGeneralizedLinearRegressionPredictSpell())
  val ev = pre.cast(DataFrameRegressionEvaluatorSpell().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse"))

}
