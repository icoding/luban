package grimoire.spark.transform.dataframe

import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.transform.Spell
import org.ddahl.rscala._

/**
  * Created by sjc505 on 17-7-28.
  */

class DataFrameToRSpell extends Spell[DataFrame,RClient] {
  //    R.colnames = dat.columns
  //    R.coltypes = dat.dtypes.map(_._2)
  //    R.colnumber = dat.columns.length
  //    R.rownumber = dat.collect().length
  //    R.data = dat.collect  //Array[Row]
  //    getFuncs("string")(dat.select($(inputCol)).collect(),0)
  //    dat.schema.map{case f=>(f.name,f.dataType.typeName)}
  //    if ( dat.select($(inputCol)).dtypes.map(_._2) = )
  //    R.set($(inputCol),dat.select($(inputCol)).collect.map(_.getDouble(0)))
  val R =RClient()

  def writeToR(dat:DataFrame,incol:String):Any = {
    val coltype = dat.select(incol).schema.map(f=>f.dataType.typeName)
    val dt =dat.select(incol).collect

    def getTypes(coltypes:Seq[String]):Any = coltypes foreach {
      case "double" => R.set(incol,dt.map(_.getDouble(0)))
      case "int" => R.set(incol,dt.map(_.getInt(0)))
      case "string" => R.set(incol,dt.map(_.getString(0)))
    }
    getTypes(coltype)
  }

  override def transformImpl(dat: DataFrame):RClient= {

    val script = dat.schema.map{
      f =>
        s"""${f.name}=${f.name}"""
    }.mkString("da = data.frame(",",",")")
    dat.columns.foreach(a => writeToR(dat,a))
    R.eval(script)
    R
  }
}

object DataFrameToRSpell {
  def apply(json: JsValue ="""{}"""): DataFrameToRSpell =
    new DataFrameToRSpell().parseJson(json)
}

