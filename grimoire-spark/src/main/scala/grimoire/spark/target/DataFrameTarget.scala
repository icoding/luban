package grimoire.spark.target

import grimoire.configuration.param._
import org.apache.spark.sql.DataFrame
import grimoire.Implicits._
import grimoire.spark.exception.UnsupportedWriteMethodException
import grimoire.target.TargetToFile
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/2/13.
  */
class DataFrameTarget extends TargetToFile[DataFrame] with HasWriteMethod with HasTableName
  with HasDataFrameTargetMode with HasOutputHiveTable{

  override def transformImpl(dat: DataFrame): Unit = {
    $(writeMethod) match {
      case WriteMethods.PARQUET =>
        dat.write.parquet($(outputPath))
      case WriteMethods.JSON =>
        dat.write.json($(outputPath))
      case WriteMethods.CSV =>
        dat.write.csv($(outputPath))
      case WriteMethods.TABLE =>
        dat.write.mode($(dataFrameTargetMode)).saveAsTable($(outputHiveTable))
      case WriteMethods.INSERTINTO =>
        dat.write.insertInto($(tableName))
      case m:Any =>
        throw new UnsupportedWriteMethodException(m)
    }

  }
}

object DataFrameTarget{
  def apply(json: JsValue="""{}"""): DataFrameTarget =
    new DataFrameTarget().parseJson(json)
}
