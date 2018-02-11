package grimoire.operation.parser

import scala.util.parsing.combinator.RegexParsers
import grimoire.operation.ScrollHasPlan
import grimoire.Implicits._
import grimoire.dataset.Pot
import grimoire.transform.Spell

import scala.util.parsing.input.CharSequenceReader
/**
  * Created by caphael on 2017/2/21.
  */
class PlanParser(val keeper: ScrollHasPlan) extends RegexParsers with Serializable{

  override val skipWhitespace = false

  val _pSpaces = "[\\s]+".r
  val _pSpacesOpt = opt(_pSpaces)

  val _pStuffUID = "[\\w-]+".r
  val _pCastOperator = "=>"
  val _pTupleOperator = ":+"

  val _pSinglePot:Parser[Pot[Any]]= _pStuffUID ^^ {
    case n=>
      keeper.getPot(n)
  }

  val _pTuplePot:Parser[Pot[Any]] = _pSpacesOpt ~> "(" ~> _pSpacesOpt ~> repsep(_pPot,_pSpacesOpt ~> _pTupleOperator <~ _pSpacesOpt) <~ _pSpacesOpt <~ ")" <~ _pSpacesOpt ^^ {
    case l =>
      val f:(Pot[Any],Pot[Any])=>Pot[Any] = (l:Pot[Any],r:Pot[Any]) => l :+ r
      l.reduce(f)
  }

  val _pPot:Parser[Pot[Any]] = _pSinglePot | _pTuplePot

  val _pSpell:Parser[Spell[Any,Any]] = _pStuffUID ^^ {
    case n=>
      keeper.getSpell(n)
  }

  val _pPlan:Parser[PotResult] = _pSpacesOpt ~> _pPot ~ rep(_pSpacesOpt ~> _pCastOperator ~> _pSpacesOpt ~> _pSpell) <~ _pSpacesOpt ^^ {
    case pot ~ spells =>
      PotResult(pot.cast(spells.reduce(_ ~ _)))
  }

  def parsePlan(text:String) = {
    _pPlan(new CharSequenceReader(text)) match {
      case Success(r,_)=>r
      case Failure(msg,_) => "FAILURE: " + msg
      case Error(msg,_) => "ERROR: " + msg
    }
  }

}


