package grimoire.ml.transform

import grimoire.transform.Spell
import org.apache.spark.ml.util.MLWritable

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2017/3/27.
  */
abstract class ToMLWritableSpell[T : TypeTag] extends Spell[T,MLWritable]
