package grimoire.ml.linalg

import org.apache.spark.ml.linalg.DenseVector

/**
  * Created by caphael on 2017/7/18.
  */
class LabeledDenseVector(values:Array[Double],val labels:Array[String]) extends DenseVector(values) with LabeledVectorLike{
  def this(from:DenseVector,labels:Array[String]) = this(from.values,labels)

  override val labMap: Map[String, Int] = labels.zipWithIndex.toMap

  override protected def apply(label: String): Double = apply(labMap(label))

  override protected def toLabeledDense(): LabeledDenseVector = this

  override protected def toLabeledSparse(): LabeledSparseVector = LabeledSparseVector(toSparse,labels)
}

object LabeledDenseVector{
  def apply(values: Array[Double], labels: Array[String]): LabeledDenseVector = new LabeledDenseVector(values, labels)

  def apply(from: DenseVector, labels: Array[String]): LabeledDenseVector = new LabeledDenseVector(from, labels)

  def unapply(arg: LabeledDenseVector): Option[(Array[Double], Array[String])] = Some((arg.values,arg.labels))
}