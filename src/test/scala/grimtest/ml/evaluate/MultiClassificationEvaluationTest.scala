package grimoire.ml.test.evaluate

import grimoire.ml.classify.transform.{DataFrameDecisionTreeClassifierPredictSpell, DataFrameDecisionTreeClassifierTrainSpell, DataFrameMulticlassClassificationEvaluatorSpell}
import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell, DataFrameStringIndexerSpell, DataFrameVectorIndexerSpell, TakeDFSpell}
import grimoire.spark.source.dataframe.LibSvmSource
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-5.
  */
object MultiClassificationEvaluationTest {
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()
  val data = LibSvmSource("""{"InputPath":"data/mllib/sample_libsvm_data.txt"}""").
    cast(DataFrameStringIndexerSpell().setInputCol("label").setOutputCol("indexedlabel")).
    cast(DataFrameVectorIndexerSpell().setInputCol("features").setOutputCol("indexedfeatures").setMaxCategories(10))

  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false))


  val mod = train.cast(DataFrameDecisionTreeClassifierTrainSpell().setLabelCol("indexedlabel").setFeaturesCol("indexedfeatures"))

  val pre = (test :+ mod).cast(DataFrameDecisionTreeClassifierPredictSpell())
  val ev = pre.
    cast(DataFrameMulticlassClassificationEvaluatorSpell().setLabelCol("indexedlabel").setPredictionCol("prediction").setMetricName("accuracy"))


}
