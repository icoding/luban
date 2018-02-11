package grimoire.ml.linalg

/**
  * Created by caphael on 2017/7/18.
  */
trait LabeledVectorLike{
  protected val labMap:Map[String,Int]

  protected def apply(label:String):Double

  protected def toLabeledDense():LabeledDenseVector
  protected def toLabeledSparse():LabeledSparseVector

}
