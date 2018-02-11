package grimoire.ml.recommendation.transform

import grimoire.ml.configuration.param.{HasLambda, HasNumIterations, HasNumRecommend, HasRank}
import grimoire.transform.Spell
import grimoire.Implicits._
import grimoire.spark.globalSpark
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.types._
import play.api.libs.json.JsValue

class ALSDataFrameSpell extends Spell[DataFrame,DataFrame] with HasRank
  with HasLambda with HasNumIterations with HasNumRecommend{

  override def transformImpl(dat: DataFrame): DataFrame = {
    val ratings = dat.rdd.map{
      case Row(shopId,userId,rating) => Rating(shopId.asInstanceOf[Int],userId.asInstanceOf[Int],rating.asInstanceOf[Double])
    }
    val model = ALS.train(ratings,$(rank),$(numIterations),$(lambda))

    val result = ratings.map(_.user).distinct().collect.flatMap {
      user => model.recommendProducts(user,$(numRecommend))
    }

    val temp_rec = globalSpark.sparkContext.parallelize(result).map{
      case Rating(shopId,userId,rating) => (shopId+" "+userId,rating)
    }
    val temp_origin = ratings.map{
      case Rating(shopId,userId,rating) => (shopId+" "+userId,rating)
    }
    val result_toDF = temp_rec.subtractByKey(temp_origin).map(x=>(x._1.split(" "),x._2)).map{
      case (x,y) => Row(x(0),x(1),y.toDouble)
    }

    val schema = StructType(
      Seq(
        StructField(name="id_1",dataType = StringType,nullable = false),
        StructField(name="id_2",dataType = StringType,nullable = false),
        StructField(name="score",dataType = DoubleType,nullable = false)))

    val df = globalSpark.createDataFrame(result_toDF,schema)
    df.sort(df("id_1").asc,df("score").desc)
  }
}

object ALSDataFrameSpell{
  def apply(json: JsValue="""{}"""): ALSDataFrameSpell =
    new ALSDataFrameSpell().parseJson(json)
}

