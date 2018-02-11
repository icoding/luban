package grimoire.ml.exception

class ModelSourceException(arg:String) extends Exception(s"the modelsource:$arg not fit this model evalue,please check!")

object ModelSourceException{
  def apply(arg: String): ModelSourceException = new ModelSourceException(arg)
}