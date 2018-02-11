package grimtest.test




/**
  * Created by sjc505 on 17-8-1.
  */
object DataFrameScalaToRTest {
  import grimoire.Implicits._
  import grimoire.spark.globalSpark
  import org.apache.spark.sql.SparkSession
  import grimoire.spark.source.dataframe.CSVSource
  import grimoire.spark.transform.dataframe.DataFrameToRSpell
  import grimoire.spark.transform.dataframe.DataFrameToScalaSpell

  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val df = CSVSource("""{"InputPath":"data/iris.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label string"}""")
  val rdf = df.cast(DataFrameToRSpell()).cast(DataFrameToScalaSpell())
  rdf.conjure

}
