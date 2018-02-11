package grimtest.zeppelin

import grimoire.zeppelin.target.ZeppelinTableTarget
import grimoire.zeppelin.transform.ZeppelinTableSpell
import grimoire.spark.globalSpark
import grimoire.spark.source.dataframe.CSVSource
import grimoire.spark.transform.dataframe.DataFrameFilterSpell
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._


/**
  * Created by caphael on 2017/7/13.
  */
object ZeppelinDataFrameTest {


  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()
  CSVSource("""{"InputPath":"data/iris.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label string"}""").setCache(true).
    cast(DataFrameFilterSpell().setFilterExpr("label <> 'setosa'")).cast(ZeppelinTableTarget())


}
