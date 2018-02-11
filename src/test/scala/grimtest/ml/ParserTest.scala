package grimtest.ml

import scala.util.parsing.combinator.RegexParsers
import scala.util.parsing.input.CharSequenceReader

/**
  * Created by sjc505 on 17-6-30.
  */
object ParserTest extends {
  val line = "[[1],[3,2],[1,2]]"
  MyParser.parse(line)
}

object MyParser extends RegexParsers{
  val DIGITS = "\\d+".r
  val LSQB = "["
  val RSQB = "]"
  def ELEM:Parser[Any] = DIGITS | ARRAY
  def ARRAY = LSQB ~> repsep(ELEM,",") <~ RSQB ^^ {
    case l =>
      l.toArray
  }

  val line = ARRAY

  def parse(text:String) = {
    line(new CharSequenceReader(text)) match {
      case Success(r,_) => r
    }
  }
}