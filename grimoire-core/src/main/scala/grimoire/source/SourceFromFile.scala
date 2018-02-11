package grimoire.source

import grimoire.configuration.param.HasInputPath

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2017/3/31.
  */
abstract class SourceFromFile[T : TypeTag] extends Source[T] with HasInputPath
