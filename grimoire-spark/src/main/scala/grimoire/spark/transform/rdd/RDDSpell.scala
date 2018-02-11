package grimoire.spark.transform.rdd

import grimoire.transform.{HasSingleSpellLike, Spell}
import org.apache.spark.rdd.RDD
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

/**
  * Created by caphael on 2017/1/5.
  */
abstract class RDDSpell[T:TypeTag,U:TypeTag](implicit clazzT:ClassTag[T],clazzU:ClassTag[U])
  extends Spell[RDD[T],RDD[U]] with HasSingleSpellLike[T,U]{

  override def transformImpl(dat: RDD[T]): RDD[U] = {
    dat.map(single.transform(_))
  }
}
