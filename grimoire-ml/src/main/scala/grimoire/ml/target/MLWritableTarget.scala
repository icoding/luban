package grimoire.ml.target

import grimoire.target.TargetToFile
import org.apache.spark.ml.util.MLWritable
import grimoire.Implicits._
import grimoire.configuration.param.HasOutputModelTarget
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/3/27.
  */
class MLWritableTarget extends TargetToFile[MLWritable] with HasOutputModelTarget{
  override def transformImpl(dat: MLWritable): Unit = {
    val writer = if ($(overwrite)) dat.write.overwrite() else dat.write
    writer.save($(outputModelTarget))
  }
}
object MLWritableTarget{
  def apply(json:JsValue): MLWritableTarget = new MLWritableTarget().parseJson(json)
}