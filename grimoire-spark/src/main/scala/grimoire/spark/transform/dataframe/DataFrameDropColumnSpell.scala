package grimoire.spark.transform.dataframe

import grimoire.spark.configuration.param.HasDropColNames
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
  * Created by caphael on 2017/3/24.
  */
class DataFrameDropColumnSpell extends DataFrameSpell with HasDropColNames{
  override def transformImpl(dat: DataFrame): DataFrame = {
    val toFilterCols = $(dropColNames).toSet
    val cols = dat.schema.map{
      case f =>
        f.name
    }.filter{
      case n =>
        ! toFilterCols.contains(n)
    }.map(col(_))
    dat.select(cols:_*)

  }
}
