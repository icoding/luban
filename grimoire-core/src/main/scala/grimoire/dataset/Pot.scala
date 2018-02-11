package grimoire.dataset

import grimoire.configuration.ConfigLike
import grimoire.configuration.param.HasCache
import grimoire.logging.Logging
import grimoire.transform.Spell
import play.api.libs.json.{JsValue, Json}

import scala.reflect.{ClassTag, classTag}
/**
  * Created by caphael on 2016/11/18.
  */
import scala.reflect.runtime.universe._

abstract class Pot[+T : TypeTag](val depends:Seq[Pot[_]]) extends ConfigLike with HasCache with Serializable with Logging {

  def this(prev:Pot[_]) = this(Seq(prev))

  def conjure:T = {
    conjureImpl
  }

  protected def conjureImpl:T

  def conjureTo[U:TypeTag]() = conjureImpl.asInstanceOf[U]

  def ~>[U : TypeTag](spell:Spell[T,U]):Pot[U] = cast[U](spell)

  def cast[U : TypeTag](spell:Spell[T,U]):Pot[U] = {
    if ($(spell.cache)){
      new CastCachedPot[T,U](this,spell.transform)
    }else{
      new CastPot[T,U](this,spell.transform)
    }
  }

  def stuffName = typeTag[T].tpe.toString

  def toMetaJson():JsValue = {
    Json.obj(
      "Type" -> "Pot",
      "Class" -> this.getClass.getName,
      "Inside" -> stuffName,
      "Params" -> paramsMetaJson()
    )
  }
}
