package grimoire.spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{CreateArray, ScalaUDF}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, DoubleType}

/**
  * Created by caphael on 2017/6/23.
  */
package object function {

  def createArrayByColumnsFunction()={
    new UserDefinedFunction(null,new ArrayType(DoubleType,false),None){
      override def apply(exprs: Column*): Column = {
        val expr = CreateArray(exprs.map(_.expr))
        new Column(expr)
      }
    }
  }
}
