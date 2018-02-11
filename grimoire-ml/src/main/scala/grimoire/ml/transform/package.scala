package grimoire.ml

import grimoire.util.keeper.MapKeeper
import org.json4s.jackson.Json
import play.api.libs.json.JsValue

import scala.reflect.runtime.universe._
/**
  * Created by caphael on 2017/3/30.
  */
package object transform {

  type StringLabelIndexedSpell = GenericDataFrameMappingGenSpell[String]
  object StringLabelIndexedSpell{
    def apply(json: JsValue): StringLabelIndexedSpell = new StringLabelIndexedSpell().parseJson(json)
  }

  type DataFrameStringLongMappingSpell = GenericDataFrameMappingSpell[String,Long]
  object DataFrameStringLongMappingSpell{
    def apply(json:JsValue): DataFrameStringLongMappingSpell = new DataFrameStringLongMappingSpell().parseJson(json)
  }

  type DataFrameLongStringMappingSpell = GenericDataFrameMappingSpell[Long,String]
  object DataFrameLongStringMappingSpell{
    def apply(json:JsValue): DataFrameLongStringMappingSpell = new DataFrameLongStringMappingSpell().parseJson(json)
  }

  type GenericMapKeeperReverseSpell = MapKeeperReverseSpell[Any,Any]
  object GenericMapKeeperReverseSpell{
    def apply(json: JsValue): GenericMapKeeperReverseSpell = new GenericMapKeeperReverseSpell().parseJson(json)
  }

  type StringLongMapKeeperReverseSpell = MapKeeperReverseSpell[String,Long]
  object StringLongMapKeeperReverseSpell{
    def apply(json: JsValue): StringLongMapKeeperReverseSpell = new StringLongMapKeeperReverseSpell().parseJson(json)
  }


}
