package grimoire.spark.deserializer

import grimoire.util.deserializer.DataDeserializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
  * Created by caphael on 2017/3/23.
  */
abstract class RowDeserializer[T] extends DataDeserializer[T,Row]{
  val dfSchema:StructType
}
