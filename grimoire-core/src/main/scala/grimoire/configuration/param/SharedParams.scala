package grimoire.configuration.param

import grimoire.configuration.ConfigLike
import grimoire.configuration.param.ParamValidators._
import grimoire.configuration.param.WriteMethods.method
import grimoire.configuration.JsonFormatFactory._
import grimoire.Implicits._
import grimoire.util.parsing.SchemaParser
import org.apache.spark.sql.types._
import play.api.data.validation.ValidationError
import play.api.libs.json._

/**
  * Created by caphael on 2017/2/14.
  */
trait HasCache extends ConfigLike {

  final val cache: Param[Boolean] = new Param[Boolean](this,"Cache", "Whether cache the output dataset.(Default:false)") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(false)

  final def setCache(c:Option[Boolean]):this.type = set(cache,c)
}

trait HasDropLast extends ConfigLike{
  final val dropLast = new Param[Boolean](this,"dropLast","Whether to drop the last category in the encoded vector") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(true)

  def setDropLast(a:Option[Boolean]):this.type = set(dropLast,a)
}


trait HasCustomDictPaths extends ConfigLike {

  final val customDictPaths: Param[Seq[String]] = new Param[Seq[String]](this,"CustomDictPaths", "Paths of Custom Dict File") {
    @transient override val format: Format[Option[Seq[String]]] = FORMAT.SEQ.STRING
  }

//  final def setCustomDictPaths(paths:Seq[String]):this.type = set(customDictPaths,paths)
  final def setCustomDictPaths(paths:Option[Seq[String]]):this.type = set(customDictPaths,paths)
}

trait HasInputCol extends ConfigLike {

  final val inputCol: Param[String] = new Param[String](this,"InputCol", "input column name") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.required()

//  final def setInputCol(col:String) : this.type  = set(inputCol,col)
  final def setInputCol(col:Option[String]) : this.type  = set(inputCol,col)
}

trait HasStopWordsPath extends ConfigLike {

  final val stopWordsPath: Param[String] = new Param[String](this,"StopWords", "stopwords dict path") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("data/stopword.txt")

  final def setStopWords(cols:Option[String]) : this.type  = set(stopWordsPath,cols)
}


trait HasInputCols extends ConfigLike {

  final val inputCols: Param[Seq[String]] = new Param[Seq[String]](this,"InputCols", "names of input columns") {
    @transient override val format: Format[Option[Seq[String]]] = FORMAT.SEQ.STRING
  }

  //  final def setInputCol(col:String) : this.type  = set(inputCol,col)
  final def setInputCols(cols:Option[Seq[String]]) : this.type  = set(inputCols,cols)
}


trait HasInputFormat extends ConfigLike {
  final val inputFormat: Param[String] = new Param[String](this,"InputFormat", "input format") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }
  final def setInputFormat(f:Option[String]):this.type = set(inputFormat,f)
}

trait HasInputPath extends ConfigLike {

  final val inputPath: Param[String] = new Param[String](this,"InputPath", "input path",notNull[String]) {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }
//  final def setInputPath(p:String):this.type = set(inputPath,p)
  final def setInputPath(p:Option[String]):this.type = set(inputPath,p)
}

trait HasInputModelSource extends ConfigLike {

  final val inputModelSource: Param[String] = new Param[String](this,"InputModelSource", "input model path",notNull[String]) {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }
  final def setInputModelSource(p:Option[String]):this.type = set(inputModelSource,p)
}

trait HasInputHiveTable extends ConfigLike {

  final val inputHiveTable: Param[String] = new Param[String](this,"InputHiveTable", "input hive table name",notNull[String]) {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.required()
  final def setInputHiveTable(p:Option[String]):this.type = set(inputHiveTable,p)
}

trait HasInputHiveLabels extends ConfigLike {

  final val inputHiveLabels: Param[String] = new Param[String](this,"InputHiveLabels", "the labels of input hive table",notNull[String]) {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("*")
  final def setInputHiveLabels(p:Option[String]):this.type = set(inputHiveLabels,p)
}

trait HasMaxIter extends ConfigLike {

  final val maxIter: Param[Int] = new Param[Int](this,"MaxIter", "Max iteration of train",notNull[Int]) {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }.default(20)
//  final def setMaxIter(p:Int):this.type = set(maxIter,p)
  final def setMaxIter(p:Option[Int]):this.type = set(maxIter,p)
}

trait HasRegParam extends ConfigLike {

  final val regParam: Param[Double] = new Param[Double](this,"RegParam", "Regularize parameter",notNull[Double]) {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(0.0)
//  final def setRegParam(p:Double):this.type = set(regParam,p)
  final def setRegParam(p:Option[Double]):this.type = set(regParam,p)
}


trait HasNature extends ConfigLike {

  final val nature: Param[Boolean] = new Param[Boolean](this,"Nature", "whether to output nature") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(false)

  final val toFilter: Param[Boolean] = new Param[Boolean](this,"Filter", "whether to save the given natures") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(false)

  final val toDrop: Param[Boolean] = new Param[Boolean](this,"Drop", "whether to drop the given natures") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(false)
  final val natures: Param[Seq[String]] = new Param[Seq[String]](this,"Natures", "the natures to be saved or dropped") {
    @transient override val format: Format[Option[Seq[String]]] = FORMAT.SEQ.STRING
  }.default(Seq("n", "v"))

//  def setNature(b: Boolean): this.type = set(nature, b)
  def setNature(b: Option[Boolean]): this.type = set(nature, b)

  def setNatureOn(): this.type = set(nature, true)

  def setNatureOff(): this.type = set(nature, false)

  def setToFilter(b: Boolean): this.type = set(toFilter, b)

  def setToFilterOn(): this.type = set(toFilter, true)

  def setToFilterOff(): this.type = set(toFilter, false)

  def setToDrop(b: Boolean): this.type = set(toDrop, b)

  def setToDropOn(): this.type = set(toDrop, true)

  def setToDropOff(): this.type = set(toDrop, false)

  def setNatures(n: Seq[String]): this.type = set(natures, n)
}

trait HasNumField extends ConfigLike {

  final val numField: Param[Int] = new Param[Int](this,"NumField", "number of fields",ge(0)) {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }.default(0)

  def setNumField(i:Option[Int]):this.type =  set(numField,i)
//  def setNumField(i:Int):this.type =  set(numField,i)
}

trait HasOriginColumns extends ConfigLike {

  final val originColumns: Param[Boolean] = new Param[Boolean](this,"OriginColumns", "whether retain the origin columns") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(true)

  final def setOriginColumns(b:Option[Boolean]): this.type = set(originColumns,b)
//  final def setOriginColumns(b:Boolean): this.type = set(originColumns,b)
}

trait HasOutputCol extends ConfigLike {

  final val outputCol: Param[String] = new Param[String](this,"OutputCol", "output column name") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("output").required()
//  final def setOutputCol(col:String) : this.type = set(outputCol,col)
  final def setOutputCol(col:Option[String]) : this.type = set(outputCol,col)
}

trait HasOutputCols extends ConfigLike {

  final val outputCols: Param[Seq[String]] = new Param[Seq[String]](this,"OutputCols", "names of output columns") {
    @transient override val format: Format[Option[Seq[String]]] = FORMAT.SEQ.STRING
  }.default("*")
  //  final def setOutputCol(col:String) : this.type = set(outputCol,col)
  final def setOutputCol(cols:Option[Seq[String]]) : this.type = set(outputCols,cols)
}


trait HasOutputPath extends ConfigLike {

  final val outputPath: Param[String] = new Param[String](this,"OutputPath", "output path") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }

//  final def setOutputPath(output:String):this.type = set(outputPath,output)
  final def setOutputPath(output:Option[String]):this.type = set(outputPath,output)
}

trait HasOutputModelTarget extends ConfigLike {

  final val outputModelTarget: Param[String] = new Param[String](this,"OutputModelTarget", "output path") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }

  final def setOutputModelTarget(output:Option[String]):this.type = set(outputModelTarget,output)
}

trait HasOverwrite extends ConfigLike {
  final val overwrite: Param[Boolean] = new Param[Boolean](this,"Overwrite", "whether overwrite the target path") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(false)
//  final def setOverwrite(b:Boolean) : this.type = set(overwrite,b)
  final def setOverwrite(b:Option[Boolean]) : this.type = set(overwrite,b)
}

trait HasDataFrameTargetMode extends ConfigLike {

  final val dataFrameTargetMode: Param[String] = new Param[String](this,"DataFrameTargetMode", "the save methods of table target") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("overwrite")
  final def setDataFrameTargetMode(col:Option[String]) : this.type = set(dataFrameTargetMode,col)
}

trait HasSchema extends ConfigLike {

  final val schema: Param[StructType] = new Param[StructType](this,"Schema", "schema") {
    @transient override val format: Format[Option[StructType]] = new Format[Option[StructType]] {
      override def reads(json: JsValue) = (json \ name) match {
        case JsDefined(JsString(s)) =>
          JsSuccess[Option[StructType]](Some(SchemaParser.parseSchema(s)))
        case JsUndefined() =>
          JsSuccess[Option[StructType]](None)
      }

      override def writes(o: Option[StructType]): JsValue = {
        Json.obj(
          name -> JsString(SchemaParser.schemaToString($(schema)))
        )
      }
    }
  }.default(SchemaParser.parseSchema("input string"))

  /**
    * Convert schema string into [[StructType]]
    */
  protected def getDataConvert = dataConvertMapping.apply _


  private val dataConvertMapping:Map[String,String => Any] =
    Map(
      "integer" -> ((s:String) => s.toInt),
      "string" -> ((s:String) => s),
      "double" -> ((s:String) => s.toDouble)
    )

  protected var stuctType:StructType = _
//  final def setSchema(s:StructType):this.type = set(schema,s)

  final def setSchema(s:Option[StructType]):this.type = set(schema,s)
}

trait HasStopWordsDictPath extends ConfigLike {

  final val stopWordsDictPath: Param[String] = new Param[String](this,"StopWordsDictPath", "Path of Stop Words Dict File") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }

//  final def setStopWordsDictPath(path:String):this.type = set(stopWordsDictPath,path)
  final def setStopWordsDictPath(path:Option[String]):this.type = set(stopWordsDictPath,path)
}

trait HasTableName extends ConfigLike {

  final val tableName: Param[String] = new Param[String](this,"TableName", "name of table") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }

//  final def setTableName(tab:String):this.type = set(tableName,tab)
  final def setTableName(tab:Option[String]):this.type = set(tableName,tab)

}

trait HasOutputHiveTable extends ConfigLike {

  final val outputHiveTable: Param[String] = new Param[String](this,"OutputHiveTable", "name of table") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }

  final def setOutputHiveTable(tab:Option[String]):this.type = set(outputHiveTable,tab)

}

trait HasSelectIndexes extends ConfigLike {
  final val selectIndexes: Param[Seq[Int]] = new Param[Seq[Int]](this,"SelectedIndexes", "number of fields") {
    @transient override val format: Format[Option[Seq[Int]]] = FORMAT.SEQ.INT
  }.default(Seq[Int]())
  def setSelecteIndexes(a:Option[Seq[Int]]):this.type = set(selectIndexes,a)
//  def setSelecteIndexes(a:Seq[Int]):this.type = set(selectIndexes,a)
}

trait HasSeparator extends ConfigLike {

  final val separator: Param[String] = new Param[String](this,"Separator", "separator of fields") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("\\s+")
  def setSeparator(sep:Option[String]):this.type = set(separator,sep)
//  def setSeparator(sep:String):this.type = set(separator,sep)
}


trait HasSQLQuery extends ConfigLike {

  final val sqlQuery: Param[String] = new Param[String](this,"SqlQuery", "SQL query from database") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }
//  final def setSQLQuery(sqlText:String):this.type = set(sqlQuery,sqlText)
  final def setSQLQuery(sqlText:Option[String]):this.type = set(sqlQuery,sqlText)
}

trait HasWriteMethod extends ConfigLike {

  final val writeMethod: Param[WriteMethods.method] = new Param[WriteMethods.method](this,"WriteMethod", "method of data persistence") {
    @transient override val format:Format[Option[WriteMethods.method]] = new Format[Option[WriteMethods.method]] {
      override def reads(json: JsValue): JsResult[Option[method]] = (json \ name) match {
        case d:JsDefined =>
          d.get match {
            case JsString(s) => JsSuccess[Option[WriteMethods.method]](Some(WriteMethods.valueOf(s)))
            case _ => JsError(Seq(JsPath() -> Seq(ValidationError("error.expected.jsstring"))))
          }
        case u:JsUndefined =>
          JsSuccess[Option[WriteMethods.method]](None)
      }

      override def writes(o: Option[method]): JsValue = {
        Json.obj(
          name -> o.map(_.toString)
        )
      }
    }
  }.default(WriteMethods.TABLE)

//  final def setWriteMethod(m:WriteMethods.method): this.type = set(writeMethod,m)
  final def setWriteMethod(m:Option[WriteMethods.method]): this.type = set(writeMethod,m)

}
object WriteMethods extends Enumeration with Serializable{
  def valueOf(v:String):method = _map(v.toUpperCase)

  type method = Value
  val PARQUET = Value
  val JSON = Value
  val TABLE = Value
  val INSERTINTO = Value
  val CSV = Value

  val _map = values.map(x=>(x.toString,x)).toMap

}


//Immediately parameter
trait HasImmeidately extends ConfigLike{
  final val immediately = new Param[Boolean](this,"Immediately","Whether conjuring immediately. default: false"){
    @transient override private[grimoire] val format = FORMAT.BOOLEAN
  }.default(false)

  def immeidate():this.type = set(immediately,true)
}
