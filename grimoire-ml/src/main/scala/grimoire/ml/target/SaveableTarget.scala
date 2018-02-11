package grimoire.ml.target

import grimoire.ml.model.SaveableMLWritableOps
import grimoire.ml.transform.ToMLWritableSpell
import grimoire.target.TargetToFile
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.mllib.util.Saveable
import grimoire.Implicits._

/**
  * Created by caphael on 2017/3/27.
  */
class SaveableTarget extends TargetToFile[Saveable]{

  val spell1 = new ToMLWritableSpell[Saveable] {
    override def transformImpl(dat: Saveable): MLWritable = {
      new SaveableMLWritableOps(dat)
    }
  }

  val spell2 = new MLWritableTarget

  override def transformImpl(dat: Saveable): Unit = {
    spell2.setOutputPath($(outputPath)).setOverwrite($(overwrite))
    (spell1 ~ spell2).transform(dat)
  }
}
