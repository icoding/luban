package grimoire.source

import grimoire.configuration.param.HasCache
import grimoire.dataset.Pot
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import play.api.libs.json.{JsString, JsSuccess, JsValue, __}

import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2017/3/31.
  */
abstract class Source [T : TypeTag] extends Pot[T](Seq()) with HasCache{

  var isCached:Boolean = false
  var cachedDat:T = _

  @transient val jsonTransformer = (__ \ 'Type).json.update(
    __.read[JsString].map{ _ => JsString("Source") }
  )

  override def toMetaJson(): JsValue = {
    super.toMetaJson().transform(jsonTransformer) match {
      case JsSuccess(value,_) => value
    }
  }

  override def conjure: T = {
    if ($(cache)) {
      if (!isCached){
        cachedDat = (conjureImpl match {
          case d:RDD[_] => d.cache()
          case d:DataFrame => d.cache()
          case d:Any => d
        }).asInstanceOf[T]
        isCached = true
      }
      cachedDat
    }else{
      conjureImpl
    }
  }

}
