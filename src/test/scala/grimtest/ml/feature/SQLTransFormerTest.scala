package grimtest.ml.feature

import grimoire.spark.source.dataframe.CSVSource
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.feature.transform.{DataFrameQuantileDiscretizerSpell, DataFrameSQLTransformerSpell}
/**
  * Created by sjc505 on 17-6-23.
  */
object SQLTransFormerTest {
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val sq = CSVSource("""{"InputPath":"data/iris.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label string"}""").
    cast(DataFrameSQLTransformerSpell().setStatement("SELECT *, (f1 + f2) AS f3, (f1 * f2) AS f4 FROM __THIS__"))

  val qd = CSVSource("""{"InputPath":"data/iris.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label string"}""").
    cast(DataFrameQuantileDiscretizerSpell().setInputCol("f1").setOutputCol("qd").setNumBuckets(3).setHandleInvalid("keep").setRelativeError(0.5))



}
