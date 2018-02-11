package grimoire.zeppelin.transform

import grimoire.ml.configuration.param.HasThreshold
import grimoire.transform.Spell
import play.api.libs.json.JsValue
import grimoire.Implicits.jstr2JsValue
import grimoire.ml.statistics.result.MultivariateStatisticalSummaryResult

/**
  * Created by sjc505 on 17-7-25.
  */
class ZeppelinSummaryResultSpell extends Spell[MultivariateStatisticalSummaryResult,String]
  with HasThreshold{

  override def transformImpl(dat: MultivariateStatisticalSummaryResult): String = {
    val decl = "%md\n"

    val header = (" "+:"f1"+:"f2"+:"f3"+:"f4"+:"").mkString("|","|","|")
    val second = (" "+:"f1"+:"f2"+:"f3"+:"").map(_=> "---").mkString("|","|","|")
    val count = (s"**${"count"}**" +: Seq(dat.count)).mkString("|","|","|")
    val max = (s"**${"max"}**" +: dat.max.toArray.map(x=>if(x>$(threshold)) s"`${x}`" else x)).mkString("|","|","|")
    val min = (s"**${"min"}**" +: dat.min.toArray.map(x=>if(x>$(threshold)) s"`${x}`" else x)).mkString("|","|","|")
    val mean = (s"**${"mean"}**" +: dat.mean.toArray.map(x=>if(x>$(threshold)) s"`${x}`" else x)).mkString("|","|","|")

    Seq(header,second,count,max,min,mean).mkString(decl,"\n","")
  }
}

object ZeppelinSummaryResultSpell{
  def apply(json: JsValue ="""{}"""): ZeppelinSummaryResultSpell =
    new ZeppelinSummaryResultSpell().parseJson(json)
}
