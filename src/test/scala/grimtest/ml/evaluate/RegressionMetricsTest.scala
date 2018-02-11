package grimoire.ml.test.evaluate

import grimoire.ml.classify.transform.{DataFrameLinearRegressionPredictSpell, DataFrameLinearRegressionTrainSpell}
import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell, TakeDFSpell}
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.LibSvmSource
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.evaluate.DataFrameRegressionMetricsSpell


/**
  * Created by sjc505 on 17-7-19.
  */
object RegressionMetricsTest {
  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val data = LibSvmSource("""{"InputPath":"data/mllib/sample_linear_regression_data.txt"}""")
  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false))

  val mod = train.cast(DataFrameLinearRegressionTrainSpell().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).
    setLabelCol("label").setFeaturesCol("features"))

  val pre = (test:+mod).cast(DataFrameLinearRegressionPredictSpell())
  val ev =pre.cast(DataFrameRegressionMetricsSpell().setPredictionCol("prediction").setLabelCol("label"))

}
