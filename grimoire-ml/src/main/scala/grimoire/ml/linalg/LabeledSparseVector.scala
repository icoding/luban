package grimoire.ml.linalg

import org.apache.spark.ml.linalg.{DenseVector, SparseVector}

/**
  * Created by caphael on 2017/7/18.
  */
class LabeledSparseVector(size: Int,
                          indices: Array[Int],
                          values: Array[Double],
                          val labels:Array[String]
                         ) extends SparseVector(size,indices,values) with LabeledVectorLike{
  override val labMap:Map[String,Int] = labels.zipWithIndex.toMap

  def this(from:SparseVector, labels:Array[String]) =
    this(from.size,from.indices,from.values,labels)

  override def apply(label:String): Double = apply(labMap(label))

  override protected def toLabeledDense(): LabeledDenseVector = LabeledDenseVector(toDense,labels)

  override protected def toLabeledSparse(): LabeledSparseVector = this

}

object LabeledSparseVector{
  def apply(size: Int,
            indices: Array[Int],
            values: Array[Double],
            labels: Array[String]
           ): LabeledSparseVector =
    new LabeledSparseVector(size, indices, values, labels)

  def apply(from: SparseVector, labels: Array[String]): LabeledSparseVector =
    new LabeledSparseVector(from, labels)

  def unapply(arg: LabeledSparseVector): Option[(Int, Array[Int], Array[Double], Array[String])] =
    Some((arg.size,arg.indices,arg.values,arg.labels))

}
