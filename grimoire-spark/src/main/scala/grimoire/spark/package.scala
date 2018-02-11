package grimoire

import grimoire.spark.exception.SparkSessionUninitializedException
import org.apache.spark.sql.SparkSession

/**
  * Created by caphael on 2017/2/18.
  */
package object spark {
  private var _spark:SparkSession = null

  implicit def globalSpark:SparkSession = {
    if (_spark == null) throw SparkSessionUninitializedException()
    else _spark
  }

  def globalSpark_=(ss:SparkSession){
    _spark = ss
  }

}
