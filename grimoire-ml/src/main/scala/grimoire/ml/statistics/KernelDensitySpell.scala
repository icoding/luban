package grimoire.ml.statistics

import grimoire.transform.{Spell, SpellPrepareLike}
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.{HasBandwidth, HasDividedNumber, HasEstimatedPoints}

/**
  * Created by sjc505 on 17-7-12.
  */
class KernelDensitySpell extends Spell[RDD[Double],Array[Double]]
  with HasBandwidth
  with HasEstimatedPoints
  with HasDividedNumber {
  val kd = new KernelDensity()

  override protected def setup(dat: RDD[Double]): Boolean = {
    if ($(estimatedPoints).isEmpty) {
      prepareEstimatedPoints(dat)
    }
    kd
      .setBandwidth($(bandwidth))
    super.setup(dat)
  }

  def prepareEstimatedPoints(dat:RDD[Double]) = {
    val n = $(dividedN)
    val dmin = dat.min()
    val dmax = dat.max()
    val range = dmax - dmin
    val inter = range / n
    val points = (0 to n).map(dmin + _ * inter)
    setEstimatedPoints(points)
  }

  def getEstmatedPoints(): Array[Double] = $(estimatedPoints).toArray

  override def transformImpl(dat: RDD[Double]): Array[Double] = {
    kd.setSample(dat).estimate($(estimatedPoints).toArray)
  }

}

object KernelDensitySpell{
  def apply(json: JsValue="""{}"""): KernelDensitySpell =
    new KernelDensitySpell().parseJson(json)
}