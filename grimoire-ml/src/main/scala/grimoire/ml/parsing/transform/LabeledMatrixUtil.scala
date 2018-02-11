package grimoire.ml.parsing.transform

import grimoire.ml.linalg.{LabeledDenseMatrix, LabeledMatrix}
import org.apache.spark.ml.linalg.Vector

/**
  * Created by sjc505 on 17-7-25.
  */
trait LabeledMatrixUtil {
  def fromVectors(constructor:(Int,Int,Array[Double],Boolean,Array[String],Array[String])=>LabeledMatrix)(vecs:Array[Vector], rowLabels: Array[String], colLabels: Array[String],transpose:Boolean = false): LabeledMatrix ={
    require(vecs.map(_.size).distinct.length == 1,"Vectors don't has the same length!")
    val numCols = vecs.head.size
    val numRows = vecs.length
    val values = vecs.flatMap(_.toArray)

    constructor(numRows,numCols,values,transpose,rowLabels, colLabels)
  }
}
