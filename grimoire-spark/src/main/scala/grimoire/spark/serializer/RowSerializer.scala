package grimoire.spark.serializer

import grimoire.util.serializer.DataSerializer
import org.apache.spark.sql.Row

/**
  * Created by caphael on 2017/3/23.
  */
abstract class RowSerializer[T] extends DataSerializer[Row,T]{

}
