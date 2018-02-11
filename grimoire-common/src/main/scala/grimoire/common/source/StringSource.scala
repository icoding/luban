package grimoire.common.source

import grimoire.configuration.param.HasInputString
import grimoire.source.{Source}
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/31.
  */
class StringSource extends Source[String] with HasInputString{
  override def conjureImpl: String = $(inputString)
}

object StringSource extends StringSource{
  def apply(json: JsValue): StringSource = new StringSource().parseJson(json)
}
