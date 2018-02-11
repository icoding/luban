package ml.test.classify

import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession

object cross_val_test {
  globalSpark = SparkSession.
    builder().
    master("local[4]").
    appName("lr").
    enableHiveSupport().
    getOrCreate()



}
