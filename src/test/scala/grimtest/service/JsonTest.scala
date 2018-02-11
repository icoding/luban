package grimtest.service

/**
  * Created by caphael on 2017/3/31.
  */
object JsonTest extends App{
  import org.apache.spark.sql.SparkSession
  import grimoire.spark.globalSpark

  import grimoire.spark.transform.single.RowToJsonSpell
  import grimoire.common.source.StringSource
  import grimoire.Implicits._
  import grimoire.spark.transform.single.JsonToRowSpell
  import grimoire.ml.classify.source.SVMModelSource
  import grimoire.ml.classify.transform.SVMPredictSpell
  import grimoire.spark.source.dataframe.CSVSource
  import grimoire.spark.transform.dataframe.{DataFrameFilterSpell, DataFrameRowToJsonSpell}
  import org.apache.spark.mllib.classification.SVMModel
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()


//  val df = CSVSource("""{"InputPath":"data/iris.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label string"}""").
//    cast(DataFrameFilterSpell("""{}""").setFilterExpr("label <> 'setosa'"))


//  df ~> DataFrameRowToJsonSpell().setOutputCol("json") conjure

  val input = StringSource.setInputString("""{"f1":7,"f2":3.2,"f3":4.7,"f4":1.4}""").cast(JsonToRowSpell.setSchema("f1 double,f2 double,f3 double,f4 double"))

  val mod = SVMModelSource("""{}""").setInputPath("model/svm").setCache(true)
  val prediction = (input :+ mod) ~> SVMPredictSpell("""{}""").setOutputCol("pred").setFeatureCols(Seq("f1","f2","f3","f4")) ~> new RowToJsonSpell()


}
