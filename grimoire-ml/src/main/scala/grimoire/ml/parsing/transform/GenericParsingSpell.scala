package grimoire.ml.parsing.transform

import grimoire.ml.parsing.GenericParser
import grimoire.transform.Spell
import org.apache.spark.rdd.RDD

import scala.reflect.runtime.universe._

/**
  * Created by sjc505 on 17-6-30.
  */
abstract class GenericParsingSpell[T :TypeTag] extends Spell[String,T]{
  val lineParser:GenericParser[T]

  override def transformImpl(dat: String): T = {
    lineParser.parse(dat)
  }
}
