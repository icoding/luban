package grimoire.operation.parser

import grimoire.operation.ScrollHasSpell
import grimoire.transform.Spell

import scala.util.parsing.combinator.RegexParsers
import scala.util.parsing.input.CharSequenceReader

/**
  * Created by caphael on 2017/2/28.
  */
class ComboParser (val keeper:ScrollHasSpell) extends RegexParsers with Serializable{

  override val skipWhitespace = false

  val _pSpaces = "[\\s]+".r
  val _pSpacesOpt = opt(_pSpaces)

  val _pStuffUID = "[\\w-]+".r
  val _pCastOperator = "=>"
  val _pComboOperator = "~"

  val _pSpell:Parser[Spell[Any,Any]] = _pStuffUID ^^ {
    case n=>
      keeper.getSpell(n)
  }

  val _pCombo:Parser[SpellResult] = _pSpacesOpt ~> repsep(_pSpell,_pSpacesOpt ~> _pComboOperator <~ _pSpacesOpt) <~ _pSpacesOpt ^^ {
    case l =>
      SpellResult(l.reduce(_ ~ _))
  }

  def parseCombo(text:String) = {
    _pCombo(new CharSequenceReader(text)) match {
      case Success(r,_)=>r
      case Failure(msg,_) => "FAILURE: " + msg
      case Error(msg,_) => "ERROR: " + msg
    }
  }
}
