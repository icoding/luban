package grimoire.nlp

import grimoire.nlp.segment.term.GTerm

/**
  * Created by caphael on 2017/2/8.
  */
object Implicits {
  implicit def gTerm2String(t:GTerm):String = t.word
  implicit def gTermArr2StringArr(arr:Array[GTerm]) :Array[String] = arr.map(gTerm2String(_))

}
