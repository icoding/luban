package grimtest.ml.feature

/**
  * Created by sjc505 on 17-6-19.
  */
object BinarizerTest {

  import grimoire.Implicits._
  import grimoire.spark.globalSpark
  import org.apache.spark.sql.SparkSession
  import grimoire.spark.source.dataframe.CSVSource
  import grimoire.ml.feature.transform.{DataFrameBinarizerSpell, DataFrameIndexToStringSpell, DataFrameStringIndexerSpell}


  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val df = CSVSource("""{"InputPath":"data/iris.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label string"}""").
    cast(DataFrameBinarizerSpell().setInputCol("f1").setOutputCol("s1").setThreshold(4.0))

}
