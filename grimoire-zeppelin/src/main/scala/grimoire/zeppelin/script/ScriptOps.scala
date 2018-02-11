package grimoire.zeppelin.script

import grimoire.ml.evaluate.result.BinaryClassificationMetricsResult
import grimoire.ml.linalg.{LabeledDenseMatrix, LabeledMatrix}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.linalg.Vector

/**
  * Created by caphael on 2017/7/25.
  */
trait ScriptOps extends ToScript{
  def toScript(withInterpreterDecl:Boolean = true):String = {
    if (withInterpreterDecl) interpreterDecl + " " + toScriptImpl()
    else toScriptImpl()
  }

  def toScriptImpl():String
}

case class DataFrameScriptOps(df:DataFrame) extends ScriptOps with ToTableScript{
  final private val fieldSep = "\t"
  final private val lineSep = "\n"
  private var outputCols = Seq("*")

  def setOutputCols(cols:Seq[String]):this.type = {
    outputCols = cols
    this
  }

  override def toScriptImpl(): String = {
    val retdat = {
      if (outputCols == "*") df
      else df.select(outputCols.map(col(_)):_*)
    }
    val header = retdat.schema.map(_.name).mkString(fieldSep)

    interpreterDecl + " " + (header +: retdat.rdd.map{
      case row =>
        row.toSeq.mkString(fieldSep)
    }.collect).mkString(lineSep)
  }
}

case class LabeledMatrixScriptOps(mat:LabeledMatrix) extends ScriptOps with ToMDScript{
  private var threshold:Double = 0.8

  def setThreshold(t:Double):this.type = {
    threshold = t
    this
  }

  override def toScriptImpl(): String = {
    val header = (" " +: mat.colLabels).mkString("|","|","|")
    val second = (" " +: mat.colLabels).map(_=> "---").mkString("|","|","|")
    val others = mat.rowLabels.zip(mat.getRows().toSeq).map{
      case (lab,vec)=>
        (s"**${lab}**" +: vec.toArray.map(x=>if(x>=threshold) s"`${x}`" else x)).mkString("|","|","|")
    }.mkString("\n")

    Seq(header,second,others).mkString("\n")
  }
}

case class BinaryClassificationMetricsOps(res:BinaryClassificationMetricsResult)  extends ScriptOps with ToMDScript {


  override def toScriptImpl(): String = {

    val numRows = res.thresholds.collect().length
    val rowLabels = res.thresholds.collect().map(_.toString)
    val colLabels =Seq("fMeasure","precision","recall").toArray
    val arr = Array(res.fMeasureByThreshold.map(_._2), res.precisionByThreshold.map(_._2) , res.recallByThreshold.map(_._2)).flatMap(_.collect())
    val mat:LabeledMatrix =  new LabeledDenseMatrix(numRows,3,arr,true,rowLabels, colLabels)

    val labmat =LabeledMatrixScriptOps(mat).toScript()
    val ss = "***"
    val pr = ("areaUnderPR:" + res.areaUnderPR)
    val roc = ("areaUnderROC:" + res.areaUnderROC)
    Seq(labmat,ss,pr,roc).mkString("\n")

  }
}