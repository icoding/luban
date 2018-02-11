package grimoire.ml

import grimoire.ml.target.GenericMapKeeperTarget
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/30.
  */
package object source {
  type StringDoubleMapKeeperSource = MapKeeperSource[String,Double]
  object StringDoubleMapKeeperSource{
    def apply(json: JsValue): StringDoubleMapKeeperSource = new StringDoubleMapKeeperSource().parseJson(json)
  }


  type StringLongMapKeeperSource = MapKeeperSource[String,Long]
  object StringLongMapKeeperSource{
    def apply(json: JsValue): StringLongMapKeeperSource = new StringLongMapKeeperSource().parseJson(json)
  }


}
