package grimtest.ml.statistics

import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.statistics.DataFrameCorrelationsSpell
import grimoire.spark.source.dataframe.CSVSource

/**
  * Created by sjc505 on 17-7-12.
  */
object CorrelationsTest {
  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val df = CSVSource("""{"InputPath":"file:///home/wuxiang/data/iris.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label string"}""")

  val cor= df.cast(DataFrameCorrelationsSpell().setInputCols(Seq("f1","f2","f3","f4")))


}
