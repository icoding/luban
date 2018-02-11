package grimoire.common.source

import grimoire.Implicits._
import grimoire.configuration.param.HasInputPath
import grimoire.source.SourceFromFile

/**
  * Created by caphael on 2016/12/12.
  */
class LocalTextFileSource extends SourceFromFile[Iterator[String]]{
  override def conjureImpl: Iterator[String] = {
    scala.io.Source.fromFile($(inputPath), "utf-8").getLines
  }
}

object LocalTextFileSource {
  def apply(path:String): LocalTextFileSource = new LocalTextFileSource().setInputPath(path)
}
