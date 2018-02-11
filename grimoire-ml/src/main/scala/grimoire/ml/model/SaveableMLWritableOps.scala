package grimoire.ml.model

import grimoire.spark.globalSpark
import org.apache.spark.ml.util.{MLWritable, MLWriter}
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.util.Saveable

/**
  * Created by caphael on 2017/3/27.
  */
class SaveableMLWritableOps(svmod:Saveable) extends MLWritable with Serializable{
  override def write: MLWriter = new MLWriter {
    override protected def saveImpl(path: String): Unit = {
      svmod.save(globalSpark.sparkContext,path)
    }
  }
}

object SaveableMLWritableOps{
  def apply(svmmod: SVMModel): SaveableMLWritableOps = new SaveableMLWritableOps(svmmod)
}
