package grimoire.ml.dimensionalityreduction


import grimoire.transform.Spell
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.ml.linalg.Vector
import grimoire.ml.Implicits._
import org.apache.spark.mllib.linalg.Vectors
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-12.
  */
class SingularValueDecompositionSpell extends Spell[RDD[Vector],Vector]{

  override def setup(dat: RDD[Vector]): Boolean = {
    val mat: RowMatrix = new RowMatrix(dat)
    super.setup(dat)
  }
  override def transformImpl(dat: RDD[Vector]): Vector = {
    val mat: RowMatrix = new RowMatrix(dat)
    mat.computeSVD(5,computeU = true).s
//    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(5,computeU = true)
//    svd.s
//    svd.U
//    svd.V
  }
}
object SingularValueDecompositionSpell{
  def apply(json: JsValue="""{}"""): SingularValueDecompositionSpell =
    new SingularValueDecompositionSpell().parseJson(json)
}