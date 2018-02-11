package grimoire.common.transform

/**
  * Created by caphael on 2016/12/8.
  */

import grimoire.Implicits._
import grimoire.transform.HasSingleSpellLike
import play.api.libs.json.JsValue

class IteratorLineSplitSpell
  extends LineSplitLike[Iterator[String],Iterator[Seq[String]]]
  with HasSingleSpellLike[String,Seq[String]]{

  override val single = new LineSplitSpell

  override def transformImpl(dat: Iterator[String]): Iterator[Seq[String]] = {
    dat.map(single.transformImpl(_))
  }

  override def parseJson(json: JsValue): IteratorLineSplitSpell.this.type = {
    single.parseJson(json)
    super.parseJson(json)
  }
}

object IteratorLineSplitSpell{
  def apply(json:String): IteratorLineSplitSpell =
    new IteratorLineSplitSpell().parseJson(json)
}