package grimoire.spark.configuration.param

import grimoire.configuration.ConfigLike
import grimoire.configuration.param.Param
import grimoire.configuration.JsonFormatFactory._
import play.api.libs.json.{JsPath, Format}
/**
  * Created by caphael on 2017/3/23.
  */
trait HasSelectColNames extends ConfigLike{
  val selectColNames = new Param[Seq[String]](this,"SelectColNames","Selected columns.") {
    @transient override val format: Format[Option[Seq[String]]] = FORMAT.SEQ.STRING
  }.default(Seq("*"))

  def setSelectColNames(a:Option[Seq[String]]):this.type = set(selectColNames,a)
}

trait HasDropColNames extends ConfigLike {
  val dropColNames = new Param[Seq[String]](this,"DropColNames","Columns for drop.") {
    @transient override val format: Format[Option[Seq[String]]] = FORMAT.SEQ.STRING
  }.default(Seq[String]())

  def setDropColNames(a:Option[Seq[String]]):this.type = set(dropColNames,a)
}

trait HasFilterExpr extends ConfigLike {
  val filterExpr = new Param[String](this,"FilterExpr","DataFrame Filter Expression.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }

  def setFilterExpr(a:Option[String]):this.type = set(filterExpr,a)
}