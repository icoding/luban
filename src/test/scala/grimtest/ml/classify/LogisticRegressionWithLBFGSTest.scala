package grimtest.ml.classify

import grimoire.ml.evaluate.DataFrameBinaryClassificationEvaluatorSpell
import grimoire.ml.feature.transform.DataFrameStringIndexerSpell


/**
  * Created by jax on 17-6-29.
  */
object LogisticRegressionWithLBFGSTest {

  import grimoire.Implicits._
  import grimoire.spark.globalSpark
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import grimoire.ml.classify.transform.LogisticRegressionWithLBFGSTrainSpell
  import grimoire.ml.transform.StringLabelIndexedSpell
  import grimoire.ml.classify.transform.DataFrameLBFGSPredictSpell
  import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell, TakeDFSpell}
  import grimoire.ml.transform.{GenericDataFrameMappingSpell, StringLongMapKeeperReverseSpell}
  import grimoire.spark.source.dataframe.CSVSource
  import grimoire.spark.transform.dataframe.DataFrameFilterSpell

  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val data = CSVSource("""{"InputPath":"data/iris.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label string"}""").
    cast(DataFrameFilterSpell("""{}""").setFilterExpr("label <> 'setosa'"))

  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false)).cast(DataFrameStringIndexerSpell().setInputCol("label").setOutputCol("indexedlabel"))


  val labmap = train.cast(StringLabelIndexedSpell("""{}""").setInputCol("label"))

  val mod = (train :+ labmap).cast(LogisticRegressionWithLBFGSTrainSpell("""{}""").setNumClasses(10))

  val pre = (test :+ mod).cast(DataFrameLBFGSPredictSpell("""{"FeatureCols":["f1","f2","f3","f4"],"OutputCol":"pred"}"""))

  val ret = (pre :+ labmap.cast(StringLongMapKeeperReverseSpell("""{}"""))).cast(new GenericDataFrameMappingSpell[Long,String]().setInputCol("pred").setOutputCol("predlab"))

  val ev = pre.cast(DataFrameBinaryClassificationEvaluatorSpell().setLabelCol("indexedlabel").setRawPredictionCol("pred").setMetricName("areaUnderROC"))

}
