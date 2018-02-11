package grimoire.ml.FPMining

import grimoire.ml.configuration.param.{HasFeaturesCol, HasMinConfidence, HasMinSupport}
import grimoire.transform.Spell
import grimoire.Implicits._
import grimoire.spark.globalSpark
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.types._
import play.api.libs.json.JsValue

class FPGrowthDataFrameSpell extends Spell[DataFrame,DataFrame] with HasMinSupport
  with HasFeaturesCol with HasMinConfidence{
  val fpg = new FPGrowth()
  override def setup(dat:DataFrame): Boolean = {
    fpg
      .setMinSupport($(minSupport))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    val df2Rdd = dat.select($(featuresCol)).rdd.map {
      case Row(x) => x.asInstanceOf[String]
    }.map(_.trim.split(" "))
    val model = fpg.run(df2Rdd)

    val temp = model.generateAssociationRules($(minConfidence)).filter(_.antecedent.length==1).
      map(x=>(x.antecedent.mkString(""),x.consequent.mkString(""),x.confidence)).sortBy(x=> - x._3).
      map(x=>Row(x._1.toString,x._2.toString,x._3.toDouble))

    val schema = StructType(
      Seq(
        StructField(name="antecedent",dataType = StringType,nullable = false),
        StructField(name="consequent",dataType = StringType,nullable = false),
        StructField(name="confidence",dataType = DoubleType,nullable = false)))
    globalSpark.createDataFrame(temp,schema)
  }
}

object FPGrowthDataFrameSpell{
  def apply(json: JsValue="""{}"""): FPGrowthDataFrameSpell =
    new FPGrowthDataFrameSpell().parseJson(json)
}

