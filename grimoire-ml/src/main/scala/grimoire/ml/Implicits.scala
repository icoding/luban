package grimoire.ml

/**
  * Created by caphael on 2017/3/23.
  */

import grimoire.ml.linalg.{LabeledDenseVector, LabeledSparseVector}
import grimoire.util.keeper.{BiMapKeeper, GenericMapKeeper, MapKeeper}
import org.apache.spark.grimoire.types.LabeledVectorUDT
import org.apache.spark.ml.linalg.{Vector => MLV, Vectors => MLVS}
import org.apache.spark.ml.feature.{LabeledPoint => MLLP}
import org.apache.spark.mllib.linalg.{Vector => MLLIBV, Vectors => MLLIBVS}
import org.apache.spark.mllib.regression.{LabeledPoint => MLLIBLP}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{Map => MMap}

object Implicits {
  implicit def toMLVector(v:MLLIBV):MLV = v.asML
  implicit def toRDDMLVector(rdd:RDD[MLLIBV]):RDD[MLV] = rdd.map(v=>v)

  implicit def toMLlibVector(v:MLV):MLLIBV = MLLIBVS.fromML(v)
  implicit def toRDDMLlibVector(rdd:RDD[MLV]):RDD[MLLIBV] = rdd.map(v=>v)

  implicit def toMLLabeledPoint(p:MLLIBLP):MLLP = MLLP(p.label,p.features)

  implicit def toMLlibLabeledPoint(p:MLLP):MLLIBLP = MLLIBLP(p.label,p.features)

  implicit def toMLlibLabeledPointRDD(rdd:RDD[MLLP])(implicit cache:Boolean = true):RDD[MLLIBLP] = {
    val ret = rdd.map(toMLlibLabeledPoint(_))
    if (cache) ret.cache()
    else ret
  }

  implicit def toDoubleValueMapKeeper(k:MapKeeper[String,Long]):MapKeeper[String,Double] = {
    val entries = k.toMap().map{
      case (k,v)=>
        (k,v.toDouble)
    }.toSeq

    val m = MMap[String,Double](entries:_*)

    k match {
      case k:GenericMapKeeper[_,_] => new GenericMapKeeper[String,Double](m)
      case k:BiMapKeeper[_,_] => new BiMapKeeper[String,Double](m)
    }
  }

  LabeledVectorUDT.register()

//  Encoders
  implicit val labeledDenseVectorEncoder =  org.apache.spark.sql.Encoders.kryo[LabeledDenseVector]
  implicit val labeledSparseVectorEncoder =  org.apache.spark.sql.Encoders.kryo[LabeledSparseVector]

}
