package grimtest.spark

/**
  * Created by caphael on 2017/6/23.
  */
object ToVectorTest {
  import org.apache.spark.sql.SparkSession
  import grimoire.spark.globalSpark
  import grimoire.Implicits._
  import org.apache.spark.sql.functions.{col, udf}
  import grimoire.spark.function.createArrayByColumnsFunction
  import grimoire.ml.feature.transform.DataFrameDoubleSeqVectorizeSpell
  import grimoire.spark.source.dataframe.CSVSource
  import grimoire.spark.transform.dataframe.DataFrameCreateArraySpell

  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val df = CSVSource("""{"InputPath":"data/iris.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label string"}""")
  df.cast(DataFrameCreateArraySpell().setInputCols(Seq("f1","f2","f3","f4")).setOutputCol("seq")).cast(DataFrameDoubleSeqVectorizeSpell().setInputCol("seq").setOutputCol("vec"))
}
