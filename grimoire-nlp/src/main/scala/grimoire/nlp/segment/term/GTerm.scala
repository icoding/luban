package grimoire.nlp.segment.term

/**
  * Created by caphael on 2017/1/23.
  */
case class GTerm(word:String){
  var nature = ""



  def setNature(n:String) : this.type = {
    nature = n
    this
  }

  override def toString():String ={
    if (GTerm.natureOut) s"${word}\\${nature}" else word
  }
}

object GTerm{
  def apply(word:String,nature:String):GTerm =
    new GTerm(word).setNature(nature)
  var natureOut = false
}
