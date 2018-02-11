package grimtest.ml.statistics

import grimoire.spark.source.rdd.TextFileRDDSource
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.statistics.{DataFrameKernelDensitySpell, KernelDensitySpell}
import grimoire.spark.source.dataframe.CSVSource
import grimoire.spark.transform.rdd.RDDStringToDoubleSpell

/**
  * Created by sjc505 on 17-7-12.
  */
object KernelDensityTest {
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

//  val kd =
//    TextFileRDDSource("""{"InputPath":"data/kerneldensity.txt"}""").
//      cast(RDDStringToDoubleSpell()).
//      cast(KernelDensitySpell())

  val df = CSVSource("""{"InputPath":"data/iris.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label string"}""")
  val kd =df.cast(DataFrameKernelDensitySpell().setBandwidth(0.1).setInputCols(Seq("f1","f2","f3","f4")))

}
