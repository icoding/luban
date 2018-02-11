package grimoire.ml.linalg

import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.{DenseMatrix, SparseMatrix}

/**
  * Created by caphael on 2017/7/17.
  */


class LabeledSparseMatrix(numRows: Int,
                          numCols: Int,
                          colPtrs: Array[Int],
                          rowIndices: Array[Int],
                          values: Array[Double],
                          isTransposed: Boolean,
                          val rowLabels:Array[String],
                          val colLabels:Array[String])
  extends SparseMatrix(numRows,numCols,colPtrs,rowIndices,values,isTransposed) with LabeledMatrix{

  override val rowLabMap:Map[String,Int] = rowLabels.zipWithIndex.toMap
  override val colLabMap:Map[String,Int] = colLabels.zipWithIndex.toMap

  def this(from:SparseMatrix, rowLabels:Array[String], colLabels:Array[String]) = {
    this(
      from.numRows,
      from.numCols,
      from.colPtrs,
      from.rowIndices,
      from.values,
      from.isTransposed,
      rowLabels,
      colLabels
    )
  }

  def apply(rowLab:String, colLab:String): Double =
    apply(rowLabMap(rowLab),colLabMap(colLab))

  override protected def toLabeledDense(): LabeledDenseMatrix = LabeledDenseMatrix(toDense,rowLabels,colLabels)

  override protected def toLabeledSparse(): LabeledSparseMatrix = this

  override def getRows(): Iterator[linalg.Vector] = rowIter

  override def getCols(): Iterator[linalg.Vector] = colIter
}

object LabeledSparseMatrix{
  def apply(numRows: Int,
            numCols: Int,
            colPtrs: Array[Int],
            rowIndices: Array[Int],
            values: Array[Double],
            isTransposed: Boolean,
            rowLabels: Array[String],
            colLabels: Array[String]): LabeledSparseMatrix =
    new LabeledSparseMatrix(numRows, numCols, colPtrs, rowIndices, values, isTransposed, rowLabels, colLabels)

  def apply(from:SparseMatrix, rowLabels:Array[String], colLabels:Array[String]): LabeledSparseMatrix =
    new LabeledSparseMatrix(from,rowLabels,colLabels)

}