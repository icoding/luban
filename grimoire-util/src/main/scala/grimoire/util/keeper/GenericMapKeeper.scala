package grimoire.util.keeper
import scala.collection.mutable
import scala.reflect.runtime.universe._
/**
  * Created by caphael on 2017/3/24.
  */
class GenericMapKeeper[K,V](protected val internalMap:mutable.Map[K,V] = mutable.Map[K,V]()) extends MapKeeper[K,V]{
  override def toMap(): mutable.Map[K, V] = internalMap

}
