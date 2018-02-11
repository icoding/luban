package grimoire.configuration.param

import grimoire.configuration.ConfigLike
import grimoire.exception.{InvalidParamException, RequiredParameterException}
import play.api.libs.json._

import scala.reflect.{ClassTag, classTag}

/**
  * Created by caphael on 2017/2/14.
  */
abstract class Param[T : ClassTag](val name:String,
                                   val doc:String,
                                   val validator:T=>Boolean )
  extends Serializable{

  implicit val pame = name

  def this(parent:ConfigLike,name:String, doc:String,validator:T=>Boolean = ParamValidators.alwaysTrue){
    this(name,doc,validator)
    parent.addParam(this)
  }

  type ValueType = T
  type OptionType = Option[T]

  var isSet = false
  protected var _value:T = _
  protected var _defaultValue:T = _
  protected var _isRequired:Boolean = false

  private def set(v:T):this.type = {
    if(validate(v)){
      _value = v
      isSet = true
      actionImpl()
      this
    }else{
      throw invalidException(v)
    }
  }

  /**
    * Parameters will act by [[actionImpl]] after it be set
    * Set [[actionImpl]] by method [[action]]
    */
  private var actionImpl : PartialFunction[Unit,Unit ] = {
    case x => x
  }

  def action(act:PartialFunction[Unit,Unit]):this.type = {
    actionImpl = act
    this
  }

  private[grimoire] def set(v:Option[T]):this.type ={
    v.map{
      case x => set(x)
    }
    this
  }

  def parseJson(json:JsValue):this.type = {
    format.reads(json) match {
      case JsSuccess(v,_) => set(v)
    }
    this
  }

  private[grimoire] def getOrDefault():T = {
    if (_isRequired && !isSet)
      throw RequiredParameterException(this)
    if (isSet) _value else _defaultValue
  }

  private[grimoire] def default(d:T):this.type = {
    _defaultValue = d
    this
  }

  private def value:T = _value

  private[grimoire] def required():this.type ={
    _isRequired = true
    this
  }

  private[grimoire] def optional():this.type ={
    _isRequired = false
    this
  }

  @transient
  private[grimoire] val format:Format[Option[T]]

  private def invalidException(v:T) = new InvalidParamException[T](name,v)

  private def validate(v:T):Boolean = {
    validator(v)
  }

  def toMetaJson():JsValue = {
    Json.obj(
      "Name" -> name,
      "Type" -> classTag[T].runtimeClass.getSimpleName,
      "Doc" -> s"""${doc}${if(_isRequired) "" else s" (default: ${_defaultValue})"}""",
      "Default" -> (if (_defaultValue == null) JsNull else _defaultValue.toString),
      "Required" -> _isRequired
    )
  }

  def toJson():JsValue = format.writes(Some(getOrDefault()))

}
