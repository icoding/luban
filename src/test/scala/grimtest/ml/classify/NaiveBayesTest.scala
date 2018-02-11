package grimtest.ml.classify

import grimoire.ml.classify.transform._
import grimoire.ml.feature.transform._
import grimoire.spark.source.dataframe. LibSvmSource
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-6-28.
  */
object NaiveBayesTest {
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()
  val data = LibSvmSource("""{"InputPath":"file:///home/wuxiang/sample_libsvm_data.txt"}""")
  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false))


  val mod = train.cast(DataFrameNaiveBayesTrainSpell().setLabelCol("label").setFeaturesCol("features"))

  val pre = (test :+ mod).cast(DataFrameNaiveBayesPredictSpell())
  val ev = pre.
    cast(DataFrameMulticlassClassificationEvaluatorSpell().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy"))

}