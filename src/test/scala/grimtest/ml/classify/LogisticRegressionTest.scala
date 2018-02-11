package grimtest.ml.classify

/**
  * Created by ARNO on 17-11-6.
  */
object LogisticRegressionTest {

  import grimoire.ml.classify.transform.{DataFrameLogisticRegressionPredictSpell,  DataFrameLogisticRegressionTrainSpell}
  import grimoire.spark.source.dataframe.CSVSource
  import grimoire.spark.globalSpark
  import org.apache.spark.sql.SparkSession
  import grimoire.Implicits._
  import grimoire.ml.feature.transform.DataFrameVectorAssemblerSpell

  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()
  val df = CSVSource("""{"InputPath":"file:///home/wuxiang/chisq.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label double"}""").
    cast(DataFrameVectorAssemblerSpell().setInputCols(Seq("f1","f2","f3","f4")).setOutputCol("features"))

  val mod = df.cast(DataFrameLogisticRegressionTrainSpell().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    .setLabelCol("label").setFeaturesCol("features").setPredictionCol("pre"))

  val pre = (df :+ mod).
    cast(DataFrameLogisticRegressionPredictSpell())

}
