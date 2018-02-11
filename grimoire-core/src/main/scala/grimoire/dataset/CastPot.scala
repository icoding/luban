package grimoire.dataset

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2016/12/9.
  */
class CastPot[+T : TypeTag,+U : TypeTag](prev:Pot[T], f: T => U ) extends Pot[U](prev){
  override protected def conjureImpl: U = {
    f(prev.conjure)
  }
}
