package grimtest.source

import grimoire.spark.source.dataframe.CSVSource

/**
  * Created by caphael on 2017/6/22.
  */
object CSVTest extends App{
  import org.apache.spark.sql.SparkSession
  import grimoire.spark.globalSpark
  import grimoire.Implicits._

  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  CSVSource().setInputPath("data/iris2.csv").setSchema("features array[double],label string")

}