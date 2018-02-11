package grimoire.dataset

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2017/4/1.
  */
class CastCachedPot[+T : TypeTag,+U : TypeTag](prev:Pot[T], f: T => U ) extends Pot[U](prev){
  var isCached:Boolean = false
  val cachedDat = conjureImpl

  override protected def conjureImpl: U = {
    if (isCached) cachedDat
    else {
      isCached = true
      (f(prev.conjure) match {
        case d:RDD[_] => d.cache()
        case d:DataFrame => d.cache()
        case d:Any => d
      }).asInstanceOf[U]
    }
  }
}