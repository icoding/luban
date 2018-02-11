package grimoire.ml.transform

import grimoire.transform.Spell
import grimoire.util.keeper.MapKeeper
import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2017/3/30.
  */
class MapKeeperReverseSpell[K:TypeTag,V:TypeTag] extends Spell[MapKeeper[K,V],MapKeeper[V,K]]{
  override def transformImpl(dat: MapKeeper[K, V]): MapKeeper[V, K] = {
    dat.reverse()
  }
}