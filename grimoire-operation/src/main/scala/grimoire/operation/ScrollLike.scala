package grimoire.operation

import grimoire.dataset.Pot
import grimoire.exception.ParsingException
import grimoire.Implicits._
import play.api.libs.json.{JsDefined, JsString, JsUndefined, JsValue}
import grimoire.operation.parser.{ComboParser, PlanParser, PotResult, SpellResult}
import grimoire.transform.Spell

/**
  * Created by caphael on 2017/2/28.
  */

trait ScrollLike{
  def parseJson(json:JsValue):this.type
}

trait ScrollHasPot {
  var potMap:Map[String,Pot[Any]] = Map[String,Pot[Any]]()

  def parsePot(json:JsValue):this.type ={
    addPots(json \ "Declaration" \ "Pot" match {
      case JsDefined(j) => StuffFactory.loadPots(j)
    })
    this
  }

  def getPot(uid:String):Pot[Any] = {
    potMap(uid)
  }

  def addPot(entry:(String,Pot[Any])):this.type = {
    potMap += entry
    this
  }

  def addPots(pots:Map[String,Pot[Any]]):this.type  = {
    potMap ++= pots
    this
  }

}

trait ScrollHasSpell{
  var spellMap:Map[String,Spell[Any,Any]] = Map[String,Spell[Any,Any]]()

  def parseSpell(json:JsValue):this.type = {
    addSpells(json \ "Declaration" \ "Spell" match {
      case JsDefined(j) => StuffFactory.loadSpells(j)
    })
    this
  }

  def getSpell(uid:String):Spell[Any,Any] = {
    spellMap(uid)
  }

  def addSpell(entry:(String,Spell[Any,Any])):this.type = {
    spellMap += entry
    this
  }

  def addSpells(spells:Map[String,Spell[Any,Any]]):this.type  = {
    spellMap ++= spells
    this
  }
}

trait ScrollHasCombo extends ScrollHasSpell{
  val comboParser = new ComboParser(this)

  //Create Combo Spell
  def createCombo(json:JsValue):Spell[Any,Any] ={
    val uid = (json \ "UID").as[String]
    val content = (json \ "Content").as[String]
    comboParser.parseCombo(content) match {
      case SpellResult(combo) => combo.setUID(uid)
      case ex:Any => throw new ParsingException(ex.toString)
    }
  }

  def parseCombo(json: JsValue):this.type ={
    (json \ "Execution" \ "Combo").as[Seq[JsValue]].foreach{
      case jv =>
        addSpell(createCombo(jv).mapEntry)
    }
    this
  }

}

trait ScrollHasPlan extends ScrollHasPot with ScrollHasSpell{
  val planParser = new PlanParser(this)

  def parsePlan(json: JsValue): this.type = {
    (json \ "Execution" \ "Plan").as[Seq[JsValue]].foreach{
      case jv =>
        addPot(createPlan(jv).mapEntry)
    }

    this
  }

  //Create Plan
  def createPlan(json:JsValue):Pot[Any] = {
    val uid = (json \ "UID").as[String]
    val content = (json \ "Content").as[String]
    planParser.parsePlan(content) match {
      case PotResult(plan) => plan.setUID(uid)
      case ex:Any => throw new ParsingException(ex.toString)
    }
  }

}

trait ScrollHasRun extends ScrollHasPlan{
  var conjureQueue:Seq[Pot[Any]] = Seq[Pot[Any]]()

  //Parse Run
  def parseConjure(json:JsValue):this.type = {
    (json \ "Execution" \ "Conjure").asOpt[Seq[JsValue]].foreach{
      case seq =>
        seq.foreach{
          case JsString(s) => addConjure(potMap(s))
        }
    }
    this
  }

  def addConjure(plan:Pot[Any]):this.type = {
    conjureQueue :+= plan
    this
  }

  def conjureAll():Seq[Any] = {
    conjureQueue.map{
      case pot => pot.conjureImpl
    }
  }
}
