package grimtest.ml

import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession

/**
  * Created by caphael on 2017/6/19.
  */
object BinarizerTest extends App{

  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()



}
