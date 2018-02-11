package grimoire.util.json.writer

import grimoire.util.keeper.MapKeeper
import play.api.libs.json._

import scala.collection.mutable.{Map => MMap}

/**
  * Created by caphael on 2017/3/23.
  */
object JsonWriters extends MapKeeper[String,AbstractJsonWriter[_]]{

  val internalMap = MMap[String,AbstractJsonWriter[_]]()

  def addWriter(writer: AbstractJsonWriter[_]) = {
    toMap() += writer.mapEntry
  }

  val stringWriter = new AbstractJsonWriter[String]("string") {
    override def matchValueFunc: PartialFunction[Any, JsValue] = {
      case v:String => JsString(v)
      case v:Any => throwMismatchException(v)
    }
  }

  val intWriter = new AbstractJsonWriter[Int]("integer") {
    override def matchValueFunc: PartialFunction[Any, JsValue] = {
      case v:Int => JsNumber(v)
      case v:Any => throwMismatchException(v)
    }
  }
  val doubleWriter = new AbstractJsonWriter[Double]("double") {
    override def matchValueFunc: PartialFunction[Any, JsValue] = {
      case v:Double => JsNumber(v)
      case v:Any => throwMismatchException(v)
    }
  }
  val longWriter = new AbstractJsonWriter[Long]("long") {
    override def matchValueFunc: PartialFunction[Any, JsValue] = {
      case v:Long => JsNumber(v)
      case v:Any => throwMismatchException(v)
    }
  }

  val booleanWriter = new AbstractJsonWriter[Boolean]("boolean") {
    override def matchValueFunc: PartialFunction[Any, JsValue] = {
      case v:Boolean => JsBoolean(v)
      case v:Any => throwMismatchException(v)
    }
  }

  override def toMap(): MMap[String, AbstractJsonWriter[_]] = internalMap
}
