package grimoire.ml.source

import grimoire.configuration.param.HasInputModelSource
import grimoire.source.SourceFromFile
import org.apache.spark.ml.util.MLReader

import scala.reflect.runtime.universe._

/**
  * Created by caphael on 2017/2/13.
  */
abstract class MLReaderSource[T : TypeTag] extends SourceFromFile[T] with HasInputModelSource{
  protected val reader:MLReader[T]

  override def conjureImpl: T = {
    reader.load($(inputModelSource))
  }
}
