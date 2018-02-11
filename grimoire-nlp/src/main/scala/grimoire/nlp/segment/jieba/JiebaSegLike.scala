package grimoire.nlp.segment.jieba

import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import grimoire.nlp.segment.Segmenter
import grimoire.nlp.segment.term.GTerm

/**
  * Created by caphael on 2016/10/29.
  */
class SerializableJiebaSegmenter extends JiebaSegmenter with Serializable{}

trait JiebaSegLike extends Segmenter{
  private lazy val segmenter = new SerializableJiebaSegmenter()

  override def _internSeg(text:String):Array[GTerm] = {

    segmenter.process(text.trim(), SegMode.SEARCH).toArray.filter{
      case w:SegToken =>
        if (w.word.getToken.trim==""){
          false
        }else{
          true
        }
    }.map(
      w => w match {
        case w:SegToken =>
          GTerm(w.word.getToken,w.word.getTokenType)
      }
    )
  }

}


