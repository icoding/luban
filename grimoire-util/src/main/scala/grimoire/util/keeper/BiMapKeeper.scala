package grimoire.util.keeper

/**
  * Created by caphael on 2017/3/23.
  */
import scala.collection.mutable
import scala.collection.mutable.Map
import scala.reflect.runtime.universe._

class BiMapKeeper[K,V](protected val internalMap:Map[K,V] = Map[K,V]()) extends MapKeeper[K,V]{
  protected val reverseKeeper:MapKeeper[V,K] = MapKeeper[V,K](toMap().map(_.swap))

  override def get(key: K): V = internalMap(key)
  def rget(key: V): K = this.reverse.get(key)

  override def put(key: K, value: V): this.type = {
    internalMap(key) = value
    reverseKeeper.put(value -> key)
    this
  }

  override def put(kv: (K, V)): this.type = {
    internalMap += kv
    reverseKeeper.put(kv.swap)
    this
  }

  override def reverse(): MapKeeper[V, K] = reverseKeeper

  override def toMap(): mutable.Map[K, V] = internalMap
}
