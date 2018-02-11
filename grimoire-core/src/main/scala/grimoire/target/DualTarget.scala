package grimoire.target

import grimoire.configuration.param.{HasOutputPath, HasOverwrite}
import grimoire.transform.DualSpell

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2017/3/27.
  */
abstract class DualTarget[-T:TypeTag,-U:TypeTag](implicit classT:ClassTag[T],classU:ClassTag[U]) extends DualSpell[T,U,Unit] with HasOutputPath with HasOverwrite
