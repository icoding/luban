package grimoire.configuration.param

import grimoire.configuration.ConfigLike
import grimoire.configuration.JsonFormatFactory._
/**
  * Created by caphael on 2017/3/31.
  */
trait HasInputString extends ConfigLike{
  final val inputString = new Param[String](this,"InputString","Input string as source.") {
    @transient override private[grimoire] val format = FORMAT.STRING
  } required()

  def setInputString(a:Option[String]):this.type = set(inputString,a)
}
