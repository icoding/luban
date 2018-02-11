package grimoire.nlp.segment.hanlp

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import grimoire.nlp.segment.Segmenter
import grimoire.nlp.segment.term.GTerm

/**
  * Created by caphael on 2017/1/4.
  */
trait HanlpSegLike extends Segmenter{
  override def _internSeg(text:String):Array[GTerm] = {
    HanlpSegLike.segmenter.seg(text).toArray.map{
      case t:Term => GTerm(t.word).setNature(t.nature.name)
    }
  }

  override def loadUserDict(paths: Seq[String]): this.type = {
    HanLP.Config.CustomDictionaryPath = paths.toArray
    HanlpSegLike.segmenter.enableCustomDictionary(true)
    this
  }

  override def loadStopWordsDict(path: String): this.type = {
    HanLP.Config.CoreStopWordDictionaryPath = path
    this
  }


}

object HanlpSegLike{
  val segmenter = HanLP.newSegment
}
