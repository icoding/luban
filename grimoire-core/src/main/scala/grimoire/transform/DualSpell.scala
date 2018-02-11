package grimoire.transform

import play.api.libs.json.JsValue

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2017/3/27.
  */


abstract class DualSpell[-T :TypeTag,-U:TypeTag,+V : TypeTag](implicit classT:ClassTag[T],classU:ClassTag[U]) extends Spell[Any,V]{
  val spell:Spell[T,V]
  val alternative:Spell[U,V]

  override def transformImpl(dat: Any): V = {

    dat match {
      case d:T =>
        spell.transform(d)
      case d:U =>
        alternative.transform(d)
    }
  }

  override def parseJson(json: JsValue): DualSpell.this.type = {
    spell.parseJson(json)
    alternative.parseJson(json)
    super.parseJson(json)
  }

}
