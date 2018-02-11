package grimoire.configuration

import grimoire.configuration.param.{Param, ParamLike}
import JsonFormatFactory._
import play.api.libs.json._

import scala.reflect.ClassTag


/**
  * Created by caphael on 2017/2/13.
  */
trait ConfigLike {

  var params:List[Param[_]] = List[Param[_]]()

  def $[T : ClassTag](param:Param[T]):T = param.getOrDefault()

//  def set[T:ClassTag](param:ParamLike[T],value:T):this.type = {
//    param.set(value)
//    this
//  }

  protected def set[T:ClassTag](param:Param[T],value:Option[T]):this.type = {
    param.set(value)
    this
  }

  def addParam[T](param:Param[T]) : this.type ={
    params :+= param
    this
  }

  def parseJson(json:JsValue):this.type = {
    params.map(_.parseJson(json))
    this
  }

  def simpleName = this.getClass.getSimpleName
  def mapEntry():(String,this.type) = ($(uid),this)

  /**
    * Common Parameters:
    * clazz: Class of current Controller
    * uid: UID of current Controller
    */
//    Clazz
  final val clazz: Param[String] = new Param[String](this,"Class","Full class name of instance"){
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default(this.getClass.getName)

  //UID for instance
  final val uid: Param[String] = new Param[String](this,"UID", "Uniform ID of instance") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default(this.toString)

  //final def setUID(id:String):this.type = set(uid,id)
  final def setUID(id:Option[String]):this.type = set(uid,id)

  protected def paramsMetaJson():JsValue = {
    JsArray(params.map(_.toMetaJson()))
  }

  protected def paramsJson():JsValue = {
    JsArray(params.map(_.toJson()))
  }

  def toJson(includeDefault:Boolean=false):JsValue = {
    JsObject(
        {
          if (includeDefault) params
        else params.filter(_.isSet)
        }.map{
          case p=>
            p.name -> (p.toJson() \ p.name).get
        }
      )
  }
}
