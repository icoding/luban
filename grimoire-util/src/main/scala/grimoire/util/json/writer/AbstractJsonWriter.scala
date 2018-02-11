package grimoire.util.json.writer

import grimoire.util.exception.MismatchedWriterException
import play.api.libs.json.{JsNull, JsValue}

/**
  * Created by caphael on 2017/3/23.
  */
abstract class AbstractJsonWriter[+T](val typeName:String) {

  protected def matchValueFunc:PartialFunction[Any,JsValue]

  protected def throwMismatchException[U](value:U) = {
    throw MismatchedWriterException(typeName,value.getClass.getSimpleName)

  }

  def write[U >: T](name:String,value:U):(String,JsValue) = {
    val jsv = value match {
      case null => JsNull
      case o:Any => matchValueFunc(o)
    }
    name -> jsv
  }

  def mapEntry:(String,this.type) = typeName -> this

  JsonWriters.addWriter(this)
}
