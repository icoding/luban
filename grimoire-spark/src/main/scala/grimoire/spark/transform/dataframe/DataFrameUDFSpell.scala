package grimoire.spark.transform.dataframe

import grimoire.configuration.param.{HasInputCol, HasOriginColumns, HasOutputCol}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2017/1/5.
  */
private[grimoire] class DataFrameUDFSpell
  extends DataFrameSpell
    with HasInputCol
    with HasOutputCol
    with HasOriginColumns{
  override def transformImpl(dat: DataFrame): DataFrame = {
    if ($(originColumns)){
      dat.select(col("*"),udfFunc(col($(inputCol))).alias($(outputCol)))
    }else{
      dat.select(udfFunc(col($(inputCol))).alias($(outputCol)))
    }
  }

  var udfFunc:UserDefinedFunction = null

  def setUDF[T:TypeTag,U:TypeTag](func:T=>U) : this.type = {
    udfFunc = udf[U,T](func)
    this
  }

}
