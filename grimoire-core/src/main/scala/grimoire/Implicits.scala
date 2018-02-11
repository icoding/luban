package grimoire

import grimoire.dataset._
import grimoire.transform.Spell
import grimoire.util.parsing.SchemaParser
import org.apache.spark.sql.types.StructType
import play.api.libs.json.{JsValue, Json}

import scala.collection.JavaConversions.mapAsJavaMap
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2016/12/2.
  */
object Implicits {
  //Conversion Methods
  implicit def trans2transSeq[T:TypeTag,U:TypeTag](trans:Spell[T,U]) = Seq[Spell[T,U]](trans)
  implicit def scala2JavaMap = mapAsJavaMap _

  implicit def pot2Pots1[T:TypeTag](pot:Pot[T]) = Pots1[T](pot)

  implicit def jstr2JsValue(s:String):JsValue = Json.parse(s)

  implicit def str2Seq(s:String):Seq[String] = Seq(s)
  implicit def str2SeqOption(s:String):Option[Seq[String]] = toOptionType(str2Seq(s))

  implicit def toAndOps[T:TypeTag](o:T) : AndOps[T] = new AndOps(o)

  // Implicit option type conversion
  implicit def toOptionType[T](o:T):Option[T] = {
    if (o != null) Some(o)
    else None
  }

  implicit def str2Schema(s:String):StructType = SchemaParser.parseSchema(s)
  implicit def str2SchemaOpt(s:String):Option[StructType] = Some(str2Schema(s))

}
