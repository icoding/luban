package grimoire.util.truansform

import grimoire.util.{DINF, INF}

/**
  * Created by caphael on 2017/3/28.
  */
class Cutter(val breaks:Seq[Double],var labels:Seq[String],private val rightClose:Boolean) extends Serializable{

  def this(breaks:Seq[Double],rightClose:Boolean){
    this(breaks,Seq[String](),rightClose)
  }

  def this(breaks:Seq[Double],labels:Seq[String]){
    this(breaks,labels,true)
  }

  def this(breaks:Seq[Double]){
    this(breaks,Seq[String](),true)
  }


  val reverseBreaks = (breaks :+ INF).reverse.toStream
  var raiseException = true

  if (labels.isEmpty) {
    labels = ("-Inf" :: breaks.toList ::: "Inf" :: Nil).sliding(2).map{
      case a =>
        if (rightClose) a.mkString("(",",","]")
        else a.mkString("[",",",")")
    }.toArray.toSeq
  }

  val cutMap:Map[Double,String] = (breaks :+ INF).zip(labels).toMap

  checkParams()

  def apply(d:Double):String = {
    val Some(upper) = reverseBreaks.map{
      case u =>
        if (rightClose) {
          if(d<=u) Some(u) else None
        }else{
          if(d<u) Some(u) else None
        }
    }.takeWhile(!_.isEmpty).last
    cutMap(upper)
  }

  def checkParams():Unit = {
    val isAscending = breaks.sliding(2).map{
      case Seq(l,r)=>
        l<r
    }.forall(x=>x)
    assert(isAscending,s"Elements in breaks sequence must be ascending.Actual breaks sequence is ${breaks.mkString("[",",","]")}.")

    if (! labels.isEmpty)
      assert(breaks.size +1 == labels.size,s"Labels size must be size of breaks + 1. Actual: Size of breaks is ${breaks.size} and size of labels is ${labels.size}.")

  }

}
