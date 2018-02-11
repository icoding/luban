package grimoire.spark.transform.dataframe
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.CreateArray
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, DoubleType}
import play.api.libs.json.JsValue
import grimoire.Implicits.jstr2JsValue
import grimoire.configuration.param.{HasInputCols, HasOriginColumns, HasOutputCol}
import org.apache.spark.sql.functions.col

/**
  * Created by caphael on 2017/6/23.
  */
class DataFrameCreateArraySpell extends DataFrameSpell with HasOriginColumns with HasInputCols with HasOutputCol{
  val udfFunc = new UserDefinedFunction(null,new ArrayType(DoubleType,false),None){
    override def apply(exprs: Column*): Column = {
      val expr = CreateArray(exprs.map(_.expr))
      new Column(expr)
    }
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    if ($(originColumns)){
      dat.withColumn($(outputCol),udfFunc($(inputCols).map(col(_)):_*))
    }else{
      dat.select(udfFunc($(inputCols).map(col(_)):_*)).alias($(outputCol))
    }
  }
}

object DataFrameCreateArraySpell{
  def apply(json: JsValue="""{}"""): DataFrameCreateArraySpell =
    new DataFrameCreateArraySpell().parseJson(json)
}
