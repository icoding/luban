package grimoire.ml.linalg

import grimoire.ml.parsing.transform.LabeledMatrixUtil
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.DenseMatrix

/**
  * Created by caphael on 2017/7/18.
  */
class LabeledDenseMatrix( numRows: Int,
                          numCols: Int,
                          values: Array[Double],
                          isTransposed: Boolean,
                          val rowLabels:Array[String],
                          val colLabels:Array[String])
  extends DenseMatrix(numRows,numCols,values,isTransposed) with LabeledMatrix{
  def this(from:DenseMatrix,rowLabels:Array[String],colLabels:Array[String]) =
    this(from.numRows,from.numCols,from.values,from.isTransposed,rowLabels,colLabels)

  override protected val rowLabMap: Map[String, Int] = rowLabels.zipWithIndex.toMap
  override protected val colLabMap: Map[String, Int] = colLabels.zipWithIndex.toMap

  override protected def apply(rowLab: String, colLab: String): Double = apply(rowLabMap(rowLab),colLabMap(colLab))

  override protected def toLabeledDense(): LabeledDenseMatrix = this

  override protected def toLabeledSparse(): LabeledSparseMatrix = LabeledSparseMatrix(toSparse,rowLabels,colLabels)

  override def getRows(): Iterator[Vector] = rowIter

  override def getCols(): Iterator[Vector] = colIter
}

object LabeledDenseMatrix extends LabeledMatrixUtil{
  def apply(numRows: Int,
            numCols: Int,
            values: Array[Double],
            isTransposed: Boolean,
            rowLabels: Array[String],
            colLabels: Array[String]
  ): LabeledDenseMatrix = new LabeledDenseMatrix(numRows, numCols, values, isTransposed, rowLabels,colLabels)

  def apply(from: DenseMatrix, rowLabels: Array[String], colLabels: Array[String]): LabeledDenseMatrix =
    new LabeledDenseMatrix(from, rowLabels, colLabels)

  def fromColVecs(colVecs:Array[Vector], rowLabels: Array[String], colLabels: Array[String]): LabeledDenseMatrix ={
    fromVectors(apply _)(colVecs,rowLabels,colLabels,false).asInstanceOf[LabeledDenseMatrix]
  }

  def fromRowVecs(rowVecs:Array[Vector], rowLabels: Array[String], colLabels: Array[String]): LabeledDenseMatrix ={
    fromVectors(apply _)(rowVecs,rowLabels,colLabels,true).asInstanceOf[LabeledDenseMatrix]
  }


}