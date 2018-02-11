package grimtest.ml.classify

import grimoire.ml.classify.transform._
import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell,TakeDFSpell}
import grimoire.spark.source.dataframe.LibSvmSource
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._


/**
  * Created by sjc505 on 17-6-29.
  */
object OneVsRestTest {
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val data = LibSvmSource("""{"InputPath":"data/mllib/sample_multiclass_classification_data.txt"}""")
  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.8)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.2)).cast(TakeDFSpell().setTakeTrain(false))

  val mod = train.
    cast(DataFrameOneVsRestTrainSpell().setLabelCol("label").setFeaturesCol("features"))
  val pre = (test :+ mod).cast(DataFrameOneVsRestPredictSpell())

  val ev = pre.cast(DataFrameMulticlassClassificationEvaluatorSpell().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy"))
}