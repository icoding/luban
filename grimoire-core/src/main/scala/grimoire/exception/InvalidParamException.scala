package grimoire.exception

import grimoire.configuration.ConfigLike
import grimoire.configuration.param.{Param, ParamLike}

/**
  * Created by caphael on 2017/2/7.
  */
class InvalidParamException[T](name:String, v:T) extends Exception(s"Value for argument [${name}] is invalid: ${v.toString}")
