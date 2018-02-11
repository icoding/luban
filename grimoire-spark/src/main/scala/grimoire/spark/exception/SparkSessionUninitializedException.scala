package grimoire.spark.exception

/**
  * Created by caphael on 2017/3/27.
  */
class SparkSessionUninitializedException extends Exception("Spark session object is uninitialized! Please initial it first!")

object SparkSessionUninitializedException{
  def apply(): SparkSessionUninitializedException = new SparkSessionUninitializedException()
}