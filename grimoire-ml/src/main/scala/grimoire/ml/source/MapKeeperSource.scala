package grimoire.ml.source

import grimoire.ml.configuration.param.HasBiMapKeeper
import grimoire.ml.exception.IncompatibleDataTypeException
import grimoire.source.SourceFromFile
import grimoire.Implicits._
import grimoire.spark.source.dataframe.ParquetSource
import grimoire.util.keeper.{BiMapKeeper, GenericMapKeeper, MapKeeper}
import org.apache.spark.sql.Row
import play.api.libs.json.JsValue

import scala.collection.mutable.Map
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

/**
  * Created by caphael on 2017/3/24.
  */
class MapKeeperSource[K:TypeTag,V:TypeTag](implicit classK:ClassTag[K],classV:ClassTag[V]) extends SourceFromFile[MapKeeper[K,V]] with HasBiMapKeeper{
//  override type ValueType = MapKeeper[K,V]
  val dfSource = new ParquetSource()

  override def conjureImpl: MapKeeper[K, V] = {
    val entries:Array[(K,V)] = dfSource.setInputPath($(inputPath)).conjureImpl.rdd.map{
      case Row(k:K,v:V) =>
        (k,v)
      case Row(k:Any,v:Any) =>
        throw IncompatibleDataTypeException(
          s"Key:${k.getClass.getName}, Value:${v.getClass.getName}",
          s"MapKeeper")
      case d:Any => throw IncompatibleDataTypeException(
        s"${d.getClass.getName}(Not MapKeeper!)",
        s"MapKeeper")
    }.collect()

    val m =Map(entries:_*)

    if ($(biMapKeeper)) new BiMapKeeper[K,V](m)
    else new GenericMapKeeper[K,V](m)
  }

  override def parseJson(json: JsValue="""{}"""): MapKeeperSource.this.type = {
    dfSource.parseJson(json)
    super.parseJson(json)
  }
}
