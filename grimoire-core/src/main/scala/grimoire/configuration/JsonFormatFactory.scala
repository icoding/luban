package grimoire.configuration

import play.api.libs.json.{Format, JsPath}

import scala.reflect.ClassTag

/**
  * Created by caphael on 2017/3/31.
  */
object JsonFormatFactory {

  object FORMAT{
    final def STRING(implicit pname:String) = (JsPath \ pname).formatNullable[String]
    final def INT(implicit pname:String) = (JsPath \ pname).formatNullable[Int]
    final def LONG(implicit pname:String) = (JsPath \ pname).formatNullable[Long]
    final def DOUBLE(implicit pname:String) = (JsPath \ pname).formatNullable[Double]
    final def BOOLEAN(implicit pname:String) = (JsPath \ pname).formatNullable[Boolean]

    object SEQ{
      final def STRING(implicit pname:String) = (JsPath \ pname).formatNullable[Seq[String]]
      final def INT(implicit pname:String) = (JsPath \ pname).formatNullable[Seq[Int]]
      final def LONG(implicit pname:String) = (JsPath \ pname).formatNullable[Seq[Long]]
      final def DOUBLE(implicit pname:String) = (JsPath \ pname).formatNullable[Seq[Double]]
      final def BOOLEAN(implicit pname:String) = (JsPath \ pname).formatNullable[Seq[Boolean]]
    }
  }
  




}
