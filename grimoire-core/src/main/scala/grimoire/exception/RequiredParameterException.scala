package grimoire.exception

import grimoire.configuration.param.Param

/**
  * Created by caphael on 2017/3/30.
  */
class RequiredParameterException(param:Param[_]) extends Exception(s"Parameter ${param.name} is required! The value of this parameter must be assigned.")
object RequiredParameterException{
  def apply(param: Param[_]): RequiredParameterException = new RequiredParameterException(param)
}
