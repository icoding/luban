package grimoire.ml.linalg

/**
  * Created by caphael on 2017/7/18.
  */

import org.apache.spark.ml.linalg.Vector

trait LabeledMatrix {
  protected val rowLabMap: Map[String, Int]
  protected val colLabMap: Map[String, Int]
  val rowLabels: Array[String]
  val colLabels: Array[String]

  protected def apply(rowLab: String, colLab: String): Double

  protected def toLabeledDense(): LabeledDenseMatrix

  protected def toLabeledSparse(): LabeledSparseMatrix

  case class StringOps(str: String) {
    def padTo(len: Int, elem: String = " ") = str + (0 to (len - str.length)).map(_ => elem).mkString
  }

  implicit def toStringOps(str: String) = StringOps(str)

  protected def toLabeledMatrixString(ori: String): String = {
    val arr = ori.split("\n").map(_.split("\\s+"))
    val maxLen = arr.flatMap(_.map(_.length)).max

    val maxRowLabLen = rowLabels.map(_.length).max
    ("" +: rowLabels).map(_.padTo(maxRowLabLen)).zip(colLabels.map(_.padTo(maxLen)) +: arr.map(_.map(_.padTo(maxLen)))).map {
      case (rowlab, row) =>
        (rowlab +: row).mkString(" ")
    }.mkString("\n")
  }

  override def toString(): String = toLabeledMatrixString(super.toString)

  def getRows(): Iterator[Vector]
  //def getRows(): Iterator[Vector]
  def getCols(): Iterator[Vector]
}