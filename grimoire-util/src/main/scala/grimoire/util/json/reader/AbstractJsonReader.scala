package grimoire.util.json.reader

import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/21.
  */
abstract class AbstractJsonReader[+T](val typeName:String) {

  def read(json:JsValue, name:String):T

  def mapEntry():(String,this.type) = typeName -> this

  JsonReaders.addReader(this)
}
