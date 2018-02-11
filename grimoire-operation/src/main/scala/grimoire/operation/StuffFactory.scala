package grimoire.operation

import grimoire.dataset.Pot
import grimoire.Implicits._
import grimoire.transform.Spell
import play.api.libs.json.JsValue

import scala.io.{Source => IOSource}

/**
  * Created by caphael on 2017/2/18.
  */
object StuffFactory {

  var cfile:String = "conf/stuffs.conf"

  //Stuff Dictionary for mapping alias/simple name to class name
  val _stuffDict:Map[String,Map[String,String]] = loadStuffDict()

  //Create Stuffs
  def _createPot(name:String):Pot[Any] ={
    Class.forName(name).getConstructors()(0).newInstance().asInstanceOf[Pot[Any]]
  }

  def _createPot(name:String,json:JsValue):Pot[Any] = {
    _createPot(name).parseJson(json)
  }

  def createPot(name:String,json:JsValue):Pot[Any] = {
    _createPot(_stuffDict("Pot")(name)).parseJson(json)
  }

  def createPot(json:JsValue):Pot[Any] = {
    (json \ "Class").asOpt[String].map{
      case name=>
        _createPot(_stuffDict("Pot")(name)).setUID(name).parseJson(json)
    }.get
  }

  def _createSpell(name:String):Spell[Any,Any] ={
    Class.forName(name).getConstructors()(0).newInstance().asInstanceOf[Spell[Any,Any]]
  }

  def _createSpell(name:String,json:JsValue):Spell[Any,Any] = {
    _createSpell(name).parseJson(json)
  }

  def createSpell(name:String,json:JsValue):Spell[Any,Any] = {
    _createSpell(_stuffDict("Spell")(name)).parseJson(json)
  }

  def createSpell(json:JsValue):Spell[Any,Any] = {
    (json \ "Class").asOpt[String].map{
      case name =>
        _createSpell(_stuffDict("Spell")(name)).setUID(name).parseJson(json)
    }.get
  }

  def loadStuffDict():Map[String,Map[String,String]] = {
    val lastWordRegex = ".*(Spell|Target|Source)".r

    val linePat = """([\w-_]+)\s+([\w]+)\s+([\w\.]+)\s*(.+)?""".r

    IOSource.
      fromFile(cfile).
      getLines().
      filter(!_.trim.startsWith("#")).
      map{
        case linePat(a,b,c,_) =>
          (a,b,c)
      }.
      toArray.
      groupBy{
        case (_,lastWordRegex(w),_)=>
          w match {
            case "Spell" => "Spell"
            case "Target" => "Spell"
            case "Source" => "Pot"
            case _ => "Unknown"
          }
      }.
      map{
        case (k,v)=>
          val m = v.flatMap{
            case (a,b,c) => Array((a,c),(b,c))
          }.toMap
          (k,m)
      }.toMap

  }

  def stuffDict(file:String="conf/stuffs.conf"):this.type = {
    cfile = file
    this
  }

  def loadPots(json:JsValue):Map[String,Pot[Any]]={
    json.asOpt[Seq[JsValue]].map{
      case seq =>
        seq.map{
          case jv =>
            createPot(jv).mapEntry()
        }.toMap
    }.get
  }

  def loadSpells(json:JsValue):Map[String,Spell[Any,Any]]={
    json.asOpt[Seq[JsValue]].map{
      case seq =>
        seq.map{
          case jv =>
            createSpell(jv).mapEntry()
        }.toMap
    }.get
  }
}
