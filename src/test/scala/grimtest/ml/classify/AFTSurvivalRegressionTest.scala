package grimtest.ml.classify

import grimoire.Implicits._
import grimoire.ml.classify.transform.{DataFrameAFTSurvivalRegressionPredictSpell, DataFrameAFTSurvivalRegressionTrainSpell, DataFrameDecisionTreeRegressorPredictSpell}
import grimoire.ml.evaluate.DataFrameRegressionEvaluatorSpell
import grimoire.spark.source.dataframe.CSVSource
import grimoire.spark.globalSpark
import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell, DataFrameVectorAssemblerSpell, TakeDFSpell}
import org.apache.spark.sql.SparkSession
/**
  * Created by jax on 17-6-28.
  */
object AFTSurvivalRegressionTest {


  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val data = CSVSource("""{"InputPath":"data/aft.csv","Schema":"label double,censor double,feature1 double,feature2 double"}""").
    cast(DataFrameVectorAssemblerSpell().setInputCols(Seq("feature1","feature2")).setOutputCol("features"))

  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false))

  val mod =train.cast(DataFrameAFTSurvivalRegressionTrainSpell().setQuantileProbabilities(Seq(0.3,0.6)).setQuantilesCol("quantiles"))

  val pre = (test :+ mod).cast(DataFrameAFTSurvivalRegressionPredictSpell())

  val ev = pre.cast(DataFrameRegressionEvaluatorSpell().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse"))

  //  val model = mod.conjure
//
//  // Print the coefficients, intercept and scale parameter for AFT survival regression
//  println(s"Coefficients: ${model.coefficients}")
//  println(s"Intercept: ${model.intercept}")
//  println(s"Scale: ${model.scale}")
}
