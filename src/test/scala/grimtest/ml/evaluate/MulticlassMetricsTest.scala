package grimoire.ml.test.evaluate

import grimoire.spark.globalSpark
import grimoire.spark.source.rdd.TextFileRDDSource
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.classify.transform.{DataFrameGBTClassifierPredictSpell, DataFrameGBTClassifierTrainSpell, DataFrameLBFGSPredictSpell, LogisticRegressionWithLBFGSTrainSpell}
import grimoire.ml.evaluate.{DataFrameMulticlassMetricsSpell, MulticlassMetricsSpell}
import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell, DataFrameStringIndexerSpell, DataFrameVectorIndexerSpell, TakeDFSpell}
import grimoire.ml.parsing.transform.RDDDouble2DParsingSpell
import grimoire.ml.transform.StringLabelIndexedSpell
import grimoire.spark.source.dataframe.{CSVSource, LibSvmSource}
import grimoire.spark.transform.dataframe.DataFrameFilterSpell

/**
  * Created by sjc505 on 17-7-19.
  */
object MulticlassMetricsTest {
//  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()
//
//  val dt = TextFileRDDSource("""{"InputPath":"data/multiclass.txt"}""").
//    cast(RDDDouble2DParsingSpell()).
//    cast(MulticlassMetricsSpell())

  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()
  val data = LibSvmSource("""{"InputPath":"data/mllib/sample_libsvm_data.txt"}""").
    cast(DataFrameStringIndexerSpell().setInputCol("label").setOutputCol("indexedlabel")).
    cast(DataFrameVectorIndexerSpell().setInputCol("features").setOutputCol("indexedfeatures").setMaxCategories(10))

  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false))


  val mod = train.cast(DataFrameGBTClassifierTrainSpell().setLabelCol("indexedlabel").setFeaturesCol("indexedfeatures").setMaxIter(10))

  val pre = (test :+ mod).cast(DataFrameGBTClassifierPredictSpell())
  val ev = pre.cast(DataFrameMulticlassMetricsSpell().setPredictionCol("indexedlabel").setLabelCol("prediction"))
}
