package grimoire.target

import grimoire.configuration.param.{HasOutputPath, HasOverwrite}
import scala.reflect.runtime.universe._
/**
  * Created by caphael on 2017/7/14.
  */
abstract class TargetToFile[T:TypeTag] extends Target[T] with HasOutputPath with HasOverwrite
