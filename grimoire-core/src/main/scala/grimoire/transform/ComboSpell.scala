package grimoire.transform

import play.api.libs.json._

import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2017/2/13.
  */
class ComboSpell[-T:TypeTag,+U:TypeTag,+V:TypeTag](s1:Spell[T,U],s2:Spell[U,V]) extends Spell[T,V]{


  s2.comboIndexIncrement(s1.comboIdx + 1)
  comboIdx = s2.comboIdx

  override def comboIndexIncrement(n: Int): Unit = {
    s1.comboIndexIncrement(n)
    s2.comboIndexIncrement(n)
    comboIdx = s2.comboIdx
  }

  override def transformImpl(dat: T):V = {
    s2.transform(s1.transform(dat))
  }

  override def parseJson(json: JsValue): ComboSpell.this.type = {
    Seq(s1,s2).foreach{
      case s:ComboSpell[_,_,_] =>
        s.parseJson(json)
      case s:Spell[_,_] =>
        json \ s.comboIdx match {
          case JsDefined(v) => s.parseJson(v)
        }
    }
    super.parseJson(json)
  }

  override def toMetaJson(): JsValue = {

    val Seq(p1,p2) = Seq(s1,s2).map{
      case s:ComboSpell[_,_,_] => {
        s.toMetaJson() \ "Params" match {
          case JsDefined(v) => v.asInstanceOf[JsObject]
        }
      }
      case s:Spell[_,_] => {
        s.toMetaJson() \ "Params" match {
          case JsDefined(v) => Json.obj(s.getClass.getName -> v)
        }
      }
    }
    val newParams = Json.obj("Params" -> p1.deepMerge(p2))

    super.toMetaJson().asInstanceOf[JsObject].deepMerge(newParams)
  }

}