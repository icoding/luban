package grimoire.util.keeper

/**
  * Created by caphael on 2017/3/21.
  */
trait Keeper[K,V] extends Serializable{
  def get(key:K):V
  def put(key:K, value:V):this.type
  def put(kv:(K,V)):this.type
}
