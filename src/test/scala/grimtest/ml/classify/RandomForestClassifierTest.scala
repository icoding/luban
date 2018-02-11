package grimtest.ml.classify

import grimoire.ml.classify.transform._
import grimoire.ml.feature.transform._
import grimoire.spark.source.dataframe.{CSVSource, LibSvmSource}
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-6-28.
  */
object RandomForestClassifierTest {

  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()
  val data = LibSvmSource("""{"InputPath":"data/mllib/sample_libsvm_data.txt"}""").
    cast(DataFrameStringIndexerSpell().setInputCol("label").setOutputCol("indexedlabel")).
    cast(DataFrameVectorIndexerSpell().setInputCol("features").setOutputCol("indexedfeatures").setMaxCategories(10))

  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false))


  val mod = train.
    cast(DataFrameRandomForestClassifierTrainSpell().setLabelCol("indexedlabel").setFeaturesCol("indexedfeatures").setNumTrees(10))


  val pre = (test :+ mod).cast(DataFrameRandomForestClassifierPredictSpell())
  val ev = pre.
    cast(DataFrameMulticlassClassificationEvaluatorSpell().setLabelCol("indexedlabel").setPredictionCol("prediction").setMetricName("accuracy"))

}
