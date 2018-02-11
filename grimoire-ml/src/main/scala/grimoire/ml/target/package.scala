package grimoire.ml

import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/30.
  */
package object target {

  type MapKeeperTarget = GenericMapKeeperTarget[Any,Any]
  object MapKeeperTarget{
    def apply(json: JsValue): MapKeeperTarget = new MapKeeperTarget().parseJson(json)
  }

  type StringDoubleMapKeeperTarget = GenericMapKeeperTarget[String,Double]
  object StringDoubleMapKeeperTarget{
    def apply(json: JsValue): StringDoubleMapKeeperTarget = new StringDoubleMapKeeperTarget().parseJson(json)
  }

  type StringLongMapKeeperTarget = GenericMapKeeperTarget[String,Long]
  object StringLongMapKeeperTarget{
    def apply(json: JsValue): StringLongMapKeeperTarget = new StringLongMapKeeperTarget().parseJson(json)
  }
}
