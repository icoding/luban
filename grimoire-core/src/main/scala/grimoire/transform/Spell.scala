package grimoire.transform

import grimoire.configuration.ConfigLike
import grimoire.configuration.param.HasCache
import grimoire.dataset.Pot
import grimoire.logging.Logging
import play.api.libs.json.{JsValue, Json}

import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2016/12/1.
  */

abstract class Spell[-T,+U](implicit typeT:TypeTag[T],typeU:TypeTag[U]) extends ConfigLike with HasCache with Serializable  with Logging{

  var comboIdx = 0

  def comboIndexIncrement(n:Int=1) = comboIdx+=n

  //Status of grimoire.transformer's processing
  private val finished = false
  //Transformer can only work after all of "dependencies" finished
  var _dependencies:Seq[Pot[_]] = Seq[Pot[_]]()

  def transform(dat:T) : U ={
    this.setup(dat)
    this.verify(dat)
    transformImpl(dat)
  }

  def transformImpl(dat:T) : U

  protected def setup(dat:T):Boolean = true

  protected def verify(dat:T):Boolean = true

  def ~[V : TypeTag](spell:Spell[U,V]):ComboSpell[T,U,V] = {
    new ComboSpell[T,U,V](this,spell)
  }

  def inputClassName = typeTag[T].tpe.toString
  def outputClassName = typeTag[U].tpe.toString

  def toMetaJson():JsValue = {
    Json.obj(
      "Type" -> "Spell",
      "Class" -> this.getClass.getName,
      "Input" -> inputClassName,
      "Output" -> outputClassName,
      "Params" -> paramsMetaJson()
    )
  }
}

