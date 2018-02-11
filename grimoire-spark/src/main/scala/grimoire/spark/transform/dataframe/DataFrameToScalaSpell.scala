package grimoire.spark.transform.dataframe

import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.configuration.param.HasSchema
import grimoire.spark.globalSpark
import grimoire.transform.Spell
import org.ddahl.rscala._

import scala.Array._

/**
  * Created by sjc505 on 17-8-3.
  */
class DataFrameToScalaSpell extends Spell[RClient,DataFrame] with HasSchema {

//  def getCols(colname: Array[String], coltype: Array[String]): Seq[Seq[Any]] = coltype.map {
//      case "numeric" => R.evalD1("subset(dt, select = c(colname))").toSeq
//      case "factor" => R.evalS1("subset(dt, select = c(colname))").toSeq
//      case "logical" => R.evalL1("subset(iris, select = c(colname))").toSeq
//  }
//  val sdt = getCols(colnames, coltypes).toArray

  override def transformImpl(dat:RClient ): DataFrame = {

    val coltypes = dat.evalS1("unlist(lapply(as.list(da),class))")
    val colnames = dat.evalS1("colnames(da)")
    val colnumber = range(1,colnames.length+1)
    val sdt = colnumber.zip(colnames).zip(coltypes).map{
      case (n,t) => t match {
        case "numeric" => {
          dat.n =n._1
          dat.evalD1("da[,n]")
        }
        case "factor" => {
          dat.n =n._1
          dat.evalD1("da[,n]")
        }
        case "logical" => {
          dat.n =n._1
          dat.evalL1("da[,n]")
        }
      }
    }

    val rows = sdt.map(a => Row(a))
    val rdd = globalSpark.sparkContext.parallelize(rows)
    globalSpark.createDataFrame(rdd,$(schema))
  }
}

object DataFrameToScalaSpell {
  def apply(json: JsValue ="""{}"""): DataFrameToScalaSpell =
    new DataFrameToScalaSpell().parseJson(json)
}
