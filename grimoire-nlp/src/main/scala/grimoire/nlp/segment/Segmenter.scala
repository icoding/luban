package grimoire.nlp.segment

import grimoire.nlp.segment.term.GTerm

/**
  * Created by caphael on 2016/10/27.
  */

abstract class Segmenter extends Serializable{

  protected var _natureOut: Boolean = false

  def natureOut(value: Boolean):Segmenter = {
    _natureOut = value
    this
  }

  protected var _toFilter: Boolean = false

  def toFilter(value: Boolean): Segmenter = {
    _toFilter = value
    this
  }

  protected var _toDrop: Boolean = false

  def toDrop(value: Boolean): Segmenter = {
    _toDrop = value
    this
  }

  protected var _natures: Set[String] = Set("a", "vn", "ns", "n", "v")

  def natures(value: Set[String]):Segmenter = {
    _natures = value
    this
  }

  protected def _internSeg(text:String):Array[GTerm]

  protected def loadUserDict(paths: Seq[String]): this.type

  protected def loadStopWordsDict(path: String): this.type

  def postDeal(segs:Array[GTerm]):Array[GTerm] = {
    segs
  }

  def segment(text:String): Array[GTerm] ={
    filter(postDeal(_internSeg(text)))
  }

  def segmentToString(text:String):String = {
    segment(text).map{
      case g:GTerm =>
        g.word + {
          if(_natureOut){
            "/"+g.nature
          }else{
            ""
          }
        }
    }.mkString(" ")
  }

  private def filter(segs:Array[GTerm]):Array[GTerm] = {
    segs.filter{
      case t:GTerm=>
        if (this._toFilter){
          _natures.contains(t.nature)
        }else if(this._toDrop){
          !(_natures.contains(t.nature))
        }else{
          true
        }
    }
  }
}
