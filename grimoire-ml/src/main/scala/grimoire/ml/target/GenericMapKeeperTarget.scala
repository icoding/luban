package grimoire.ml.target

import grimoire.configuration.param.HasOverwrite
import grimoire.ml.transform.ToMLWritableSpell
import grimoire.util.keeper.MapKeeper
import grimoire.Implicits._
import grimoire.spark.globalSpark
import grimoire.spark.target.DataFrameTarget
import play.api.libs.json.JsValue
import grimoire.target.TargetToFile
import grimoire.util.parsing.SchemaParser
import org.apache.spark.ml.util.{MLWritable, MLWriter}
import org.apache.spark.sql.Row

import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2017/3/24.
  */
class GenericMapKeeperTarget[K:TypeTag,V:TypeTag] extends TargetToFile[MapKeeper[K,V]] with HasOverwrite{

  val toMLWrableSpell = new ToMLWritableSpell[MapKeeper[K,V]] {
    val dfTarget = new DataFrameTarget()
    override def transformImpl(dat: MapKeeper[K, V]): MLWritable = {
      new MLWritable {
        override def write: MLWriter = new MLWriter {
          override protected def saveImpl(path: String): Unit = {
            val schemaStr:String = dat.toMap().head match {
              case (k,v) =>
                s"key ${k.getClass.getSimpleName.toLowerCase()},value ${v.getClass.getSimpleName.toLowerCase()}"
            }

            val rdd = globalSpark.sparkContext.parallelize(
              dat.toMap().map{
                case (k,v)=>
                  Row(k,v)
              }.toSeq
            )
            val df = globalSpark.createDataFrame(rdd,SchemaParser.parseSchema(schemaStr))
            dfTarget.setOutputPath(path).transform(df)
          }
        }
      }
    }
  }

  val mlTarget = new MLWritableTarget

  override def transformImpl(dat: MapKeeper[K, V]): Unit ={
    mlTarget.setOutputPath($(outputPath)).setOverwrite($(overwrite))
    (toMLWrableSpell ~ mlTarget).transform(dat)
  }


  override def parseJson(json: JsValue): GenericMapKeeperTarget.this.type = {
    super.parseJson(json)
  }
}
