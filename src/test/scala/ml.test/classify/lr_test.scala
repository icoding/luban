package ml.test.classify

import grimoire.Implicits.jstr2JsValue
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.{HiveSource, LibSvmSource}
import org.apache.spark.sql.{Row, SparkSession}
import grimoire.Implicits._
import grimoire.ml.classify.transform._
import grimoire.ml.evaluate.{DataFrameBinaryClassificationMetricsSpell, DataFrameConfusionMatrix, DataFrameMulticlassMetricsSpell}
import grimoire.ml.feature.transform._
import grimoire.spark.transform.dataframe.DataFrameFilterSpell
import org.apache.spark.mllib.evaluation.MulticlassMetrics


object lr_test extends App{
  globalSpark = SparkSession.
    builder().
    master("local[4]").
    appName("lr").
    enableHiveSupport().
    getOrCreate()

  val df = HiveSource("""{"InputHiveTable":"iris2"}""").
    cast(DataFrameStringIndexerSpell("""{"InputCol":"label","OutputCol":"intlabel"}""")).
    cast(DataFrameVectorAssemblerSpell("""{"InputCols":["f1", "f2", "f3","f4"],"OutputCol":"features"}"""))

  val train = df.cast(DataFrameRandomSplitSpell().setRandomRate(0.8)).cast(TakeDFSpell())
  val test = df.cast(DataFrameRandomSplitSpell().setRandomRate(0.2)).cast(TakeDFSpell().setTakeTrain(false))

  val mod = train.cast(DataFrameLogisticRegressionTrainSpell("""{"MaxIter":10,"RegParam":0.3,"ElasticNetParam":0.8,"LabelCol":"intlabel","FeaturesCol":"features","PredictionCol":"predic","family":"multinomial"}"""))

  (test :+ mod).cast(DataFrameLogisticRegressionPredictSpell()).
    cast(DataFrameBinaryClassificationMetricsSpell().setLabelCol("intlabel").setPredictionCol("predic")).conjure

  val mod_1 = df.cast(DataFrameRandomForestClassifierTrainSpell("""{"LabelCol":"intlabel","FeaturesCol":"features","PredictionCol":"prediction","NumTrees":10}"""))

  (df :+ mod_1).cast(DataFrameRandomForestClassifierPredictSpell())





  val df1 = LibSvmSource("""{"InputPath":"file:///home/wuxiang/data/mllib/sample_libsvm_data.txt"}""")

  df1.conjure.groupBy("label").count.show //

  val df2 = df1.cast(DataFrameStringIndexerSpell("""{"InputCol":"label","OutputCol":"intlabel"}"""))

  val mod1 = df2.cast(DataFrameLogisticRegressionTrainSpell("""{"MaxIter":10,"RegParam":0.3,"ElasticNetParam":0.8,"LabelCol":"intlabel","FeaturesCol":"features","PredictionCol":"predic"}"""))

  (df2 :+ mod1).cast(DataFrameLogisticRegressionPredictSpell()).
    cast(DataFrameBinaryClassificationMetricsSpell().setLabelCol("intlabel").setPredictionCol("probability")).conjure




  val df4 = HiveSource("""{"InputHiveTable":"iris2"}""").
    cast(DataFrameStringIndexerSpell().setInputCol("label").setOutputCol("indexedlabel")).
    cast(DataFrameVectorAssemblerSpell("""{"InputCols":["f1", "f2", "f3","f4"],"OutputCol":"features"}"""))

  val train4 = df4.cast(DataFrameRandomSplitSpell().setRandomRate(0.6)).cast(TakeDFSpell())
  val test4 = df4.cast(DataFrameRandomSplitSpell().setRandomRate(0.4)).cast(TakeDFSpell().setTakeTrain(false))


  val mod4 = train4.cast(DataFrameDecisionTreeClassifierTrainSpell().setLabelCol("indexedlabel").setFeaturesCol("features").setPredictionCol("predic"))
  val pre4 = (test4 :+ mod4).cast(DataFrameDecisionTreeClassifierPredictSpell())
  pre4.cast(DataFrameConfusionMatrix().setIndexedlabelCol("indexedlabel").setPredictionCol("predic").setOriginlabelCol("label"))


  val pre5= pre4.conjure

  val dt = pre5.select("predic","indexedlabel").rdd.map{
    case Row(a:Double,b:Double) =>
      (a,b)
  }
  val mc = new MulticlassMetrics(dt)

//  val ev = pre4.
//    cast(DataFrameMulticlassMetricsSpell().setLabelCol("indexedlabel").setPredictionCol("predic")).conjure

}
