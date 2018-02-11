package grimoire.ml.parsing

import org.apache.spark.rdd.RDD

import scala.util.parsing.combinator.RegexParsers
import scala.util.parsing.input.CharSequenceReader

/**
  * Created by sjc505 on 17-6-30.
  */
abstract class GenericParser[T] extends RegexParsers with Serializable{
  val lineParser:Parser[T]

  def parse(text:String):T = {
    lineParser(new CharSequenceReader(text)) match {
      case Success(r,_) => r
    }
  }
}
