package grimoire.ml.transform

import grimoire.configuration.param.HasInputCol
import grimoire.ml.configuration.param.HasStartFromZero
import grimoire.transform.Spell
import grimoire.util.keeper.{BiMapKeeper, MapKeeper}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.Map
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2017/3/24.
  */
class GenericDataFrameMappingGenSpell[K:ClassTag](implicit typeM:TypeTag[MapKeeper[K,Long]])
  extends Spell[DataFrame,MapKeeper[K,Long]]
    with HasInputCol
    with HasStartFromZero{

  override def transformImpl(dat: DataFrame): MapKeeper[K,Long] = {
    val indexed = dat.select($(inputCol)).distinct().rdd.map{
      case r=>
        r.getAs[K](0)
    }.zipWithIndex
    val entries = if ($(startFromZero)) indexed
    else indexed.map{case (d,i) =>(d,i+1)}
    new BiMapKeeper(Map(entries.collect():_*))
  }
}
