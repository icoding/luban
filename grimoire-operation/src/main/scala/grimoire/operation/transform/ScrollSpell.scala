package grimoire.operation.transform

import grimoire.configuration.ConfigLike
import grimoire.operation.{ScrollHasCombo, ScrollLike}
import grimoire.transform.Spell
import play.api.libs.json.{JsString, JsValue}


/**
  * Created by caphael on 2017/2/27.
  */
class ScrollSpell extends Spell[Any,Any] with ConfigLike with ScrollLike with ScrollHasCombo{
  var _spell:Spell[Any,Any] = _

  override def transformImpl(dat: Any): Any = {
    _spell.transformImpl(dat)
  }

  override def parseJson(json: JsValue): ScrollSpell.this.type = {
    parseSpell(json).parseCombo(json).parseToSpell(json)

  }

  def parseToSpell(json: JsValue): this.type = {
    (json \ "Execution" \ "ToSpell").asOpt[JsValue].foreach{
      case JsString(s) =>
        _spell = spellMap(s)
    }
    this
  }
}