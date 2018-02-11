package grimoire.operation
import play.api.libs.json.JsValue
import grimoire.Implicits._
import scala.io.Source

/**
  * Created by caphael on 2017/2/27.
  */
class Scroll extends ScrollLike with ScrollHasRun with ScrollHasCombo {
  override def parseJson(json: JsValue): this.type =
    parsePot(json).
      parseSpell(json).
      parseCombo(json).
      parsePlan(json).
      parseConjure(json)
}

object Scroll{
  def load(file:String):Scroll = {
    val jstr = Source.fromFile(file).mkString
    new Scroll().parseJson(jstr)
  }

  def apply(json: JsValue): Scroll = new Scroll().parseJson(json)



}