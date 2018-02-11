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
object MultilayerPerceptronClassifierTest {
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val data = LibSvmSource("""{"InputPath":"data/mllib/sample_multiclass_classification_data.txt"}""")
  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false))

  val mod = train.
    cast(DataFrameMultilayerPerceptronClassifierTrainSpell().setLabelCol("label").setFeaturesCol("features").setMaxIter(10).setLayers(Seq(4, 5, 4, 3)).setBlockSize(128).setSeed(1234L))
  val pre = (test :+ mod).
    cast(DataFrameMultilayerPerceptronClassifierPredictSpell())
  val ev = pre.cast(DataFrameMulticlassClassificationEvaluatorSpell().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy"))
}
