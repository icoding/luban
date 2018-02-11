package grimoire.util.keeper

/**
  * Created by caphael on 2017/3/21.
  */
import scala.collection.mutable.Map
import scala.reflect.runtime.universe._

trait MapKeeper[K,V]
  extends Keeper[K,V]{

  override def get(key: K): V = toMap()(key)

  override def put(key: K, value: V): this.type = {
    toMap()(key) = value
    this
  }

  override def put(kv: (K, V)): this.type = {
    toMap += kv
    this
  }

  def reverse():MapKeeper[V,K] = MapKeeper(toMap().map(_.swap))

  def toMap():Map[K,V]
}

object MapKeeper{
  def apply[K, V](m:Map[K,V]): MapKeeper[K,V] = new GenericMapKeeper[K,V](m)
}

