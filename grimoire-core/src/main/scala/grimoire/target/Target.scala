package grimoire.target

import grimoire.transform.Spell
import play.api.libs.json.{JsString, JsSuccess, JsValue, __}
import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2017/2/13.
  */
abstract class Target[-T : TypeTag] extends Spell[T,Unit] {
  @transient val jsonTransformer = (__ \ 'Type).json.update(
    __.read[JsString].map{ _ => JsString("Target") }
  ) andThen (__ \ 'Output).json.prune

  override def toMetaJson(): JsValue = {
    super.toJson().transform(jsonTransformer) match {
      case JsSuccess(value,_) => value
    }
  }
}
