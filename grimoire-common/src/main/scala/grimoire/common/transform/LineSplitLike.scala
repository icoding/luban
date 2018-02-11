package grimoire.common.transform

import grimoire.configuration.param.{HasNumField, HasSelectIndexes, HasSeparator}
import grimoire.transform.Spell

/**
  * Created by caphael on 2016/12/12.
  */
trait LineSplitLike[T,U] extends Spell[T,U] with HasSeparator with HasNumField with HasSelectIndexes{

  override def verify(dat:T): Boolean = {
    if ($(numField) == 0 || ( $(numField) >0 && $(selectIndexes).forall(_ < $(numField)) ) ) {
      true
    }else{
      logger.error("Index of selected field was beyond the number of fields!")
      false
    }
  }
}
