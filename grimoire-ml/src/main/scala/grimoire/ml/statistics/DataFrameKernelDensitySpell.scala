package grimoire.ml.statistics

import grimoire.spark.transform.dataframe.DataFrameSpell
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.configuration.param.HasCache
import org.apache.spark.sql.functions._

import scala.math.{max => smax, min => smin}
import grimoire.ml.configuration.param.{HasBandwidth, HasDividedNumber, HasEstimatedPoints, HasInputCols}
import grimoire.spark.globalSpark
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  * Created by sjc505 on 17-7-14.
  */
class DataFrameKernelDensitySpell extends DataFrameSpell with HasBandwidth
  with HasInputCols with HasDividedNumber with HasEstimatedPoints with HasCache{

  val kds: KernelDensitySpell = new KernelDensitySpell()

  private def updateMinMax(minmax:(Double,Double),elem:Double):(Double,Double) ={
    (smin(elem,minmax._1),smax(elem,minmax._2))
  }

  override def setup(dat: DataFrame): Boolean = {
    kds
      .setBandwidth($(bandwidth))
      .setDividedNumber($(dividedN))
      .setCache($(cache))
      .setEstimatedPoints($(estimatedPoints))

    if ($(estimatedPoints).isEmpty){
      val cols = $(inputCols).flatMap{
        case colname =>
          Seq(min(colname),max(colname))
      }
      val seq = dat.select(cols:_*).first.toSeq.map{case d:Double=>d}.sortBy(d=>d)
      val n = $(dividedN)
      val dmin = seq.head
      val dmax = seq.last
      val range = dmax - dmin
      val inter = range / n
      val points = (0 to n).map(dmin + _ * inter)
      setEstimatedPoints(points)
    }

    kds.setEstimatedPoints($(estimatedPoints))

    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    val estpnts:Seq[Double] = kds.getEstmatedPoints()

    val rows:Seq[Row] = $(inputCols).flatMap{
      case colname =>
        val samples = dat.select(colname).rdd.map{case row => row.getDouble(0)}
        estpnts.zip(kds.transform(samples)).map{
          case (e,d)=>
            Row(e,d,colname)
        }
    }

    val rdd = globalSpark.sparkContext.parallelize(rows)

    val schema = StructType(
      Seq(
        StructField("points",DoubleType),
        StructField("prob",DoubleType),
        StructField("feature",StringType)
      )
    )
    globalSpark.createDataFrame(rdd,schema)
  }
}

object DataFrameKernelDensitySpell{
  def apply(json: JsValue="""{}"""): DataFrameKernelDensitySpell =
    new DataFrameKernelDensitySpell().parseJson(json)
}