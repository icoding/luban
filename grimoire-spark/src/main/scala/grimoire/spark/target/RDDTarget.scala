package grimoire.spark.target

import grimoire.target.TargetToFile
import org.apache.spark.rdd.RDD

import scala.reflect.runtime.universe._
/**
  * Created by caphael on 2017/3/24.
  */
class RDDTarget[T:TypeTag] extends TargetToFile[RDD[T]]{
  override def transformImpl(dat: RDD[T]): Unit = {
    dat.saveAsObjectFile($(outputPath))
  }
}
