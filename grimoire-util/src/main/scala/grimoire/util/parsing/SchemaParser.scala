package grimoire.util.parsing

import org.apache.spark.sql.types._

import scala.util.parsing.combinator.RegexParsers
import scala.util.parsing.input.CharSequenceReader

/**
  * Created by caphael on 2017/3/30.
  */
object SchemaParser extends RegexParsers {
  private val _pCharacters = opt("\"".r) ~> "[\\w\\.\\/\\-\\:,]+".r <~ opt("\"".r)
  private val _pElemType:Parser[DataType] = ("int" | "double" | "string" | "long" | "INT" | "DOUBLE" | "STRING" | "LONG") ^^ {
    case s=>
      getDataType(s.toLowerCase())
  }
  private val _pArrayType = ("array" | "ARRAY") ~> "[" ~> _pElemType <~ "]" ^^ {
    case t =>
      ArrayType(t)
  }
  private val _pColType = _pElemType | _pArrayType
  private val _pColDef = _pCharacters ~ _pColType ^^ { case a ~ b => (a,b)}
  private val _pSchema = repsep(_pColDef,",") ^^ {
    case l =>
      StructType{
        l map {
          case (c:String,t:DataType)=>
            StructField(c,t,true)
        }
      }
  }

  def parseSchema(s:String):StructType = {
    _pSchema(new CharSequenceReader(s)) match {
      case Success(s,_) => s
      case s:Any => throw new Exception(s"Can not parse the schema string:${s}!")
    }
  }

  def schemaToString(schema:StructType):String = {
    schema.map{
      case f =>
        s"${f.name} ${f.dataType.json.replace(""""""","")}"
    }.mkString(",")
  }

  private def getDataType:String => DataType = dataTypeMapping.apply _
  private val dataTypeMapping:Map[String,DataType] =
    Map("int" -> IntegerType, "string" -> StringType, "double" -> DoubleType, "long" -> LongType)

}