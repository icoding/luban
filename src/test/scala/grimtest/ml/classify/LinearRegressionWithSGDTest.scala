package grimtest.ml.classify

import grimoire.ml.transform.{GenericDataFrameMappingSpell, StringLongMapKeeperReverseSpell}


/**
  * Created by jax on 17-6-29.
  */
object LinearRegressionWithSGDTest {


  import grimoire.ml.transform.{GenericDataFrameMappingSpell, StringLongMapKeeperReverseSpell}
  import grimoire.Implicits._
  import grimoire.spark.globalSpark
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import grimoire.ml.classify.transform.LinearRegressionWithSGDTrainSpell
  import grimoire.ml.transform.StringLabelIndexedSpell
  import grimoire.spark.source.dataframe.CSVSource
  import grimoire.ml.classify.transform.DataFrameSGDPredictSpell
  import grimoire.ml.target.ModelTarget

  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val data = CSVSource("""{"InputPath":"data/mllib/ridge-data/lpsa.csv","Schema":"label string,f2 double,f3 double,f4 double,f5 double,f6 double,f7 double,f8 double,f9 double"}""")


  val labmap = data.cast(StringLabelIndexedSpell("""{}""").setInputCol("label"))

  val mod = (data :+ labmap).cast(LinearRegressionWithSGDTrainSpell("""{}""").setLabelCol("label").setFeatureCols("*").setStepSize(0.0000001).setNumIterations(100))

  //  mod.cast(ModelTarget("""{"OutputPath":"model/sgd","Overwrite":true}"""))

  //  val mod = SGDModelSource("""{}""").setInputPath("model/sgd")

  val pred = (data :+ mod).cast(DataFrameSGDPredictSpell("""{"FeatureCols":["f2","f3","f4","f5","f6","f7","f8","f9"],"OutputCol":"pred"}"""))
  val predc = pred.conjure.first()

}
