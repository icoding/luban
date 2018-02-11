package org.apache.spark.grimoire.types

import grimoire.ml.linalg.{LabeledDenseVector, LabeledSparseVector, LabeledVectorLike}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

/**
  * Created by caphael on 2017/7/18.
  */
@SQLUserDefinedType(udt = classOf[LabeledVectorUDT])
class LabeledVectorUDT extends UserDefinedType[LabeledVectorLike]{
  override def sqlType: DataType = structType

  val structType =  {
    // type: 0 = sparse, 1 = dense
    // We only use "values" for dense vectors, and "size", "indices", and "values" for sparse
    // vectors. The "values" field is nullable because we might want to add binary vectors later,
    // which uses "size" and "indices", but not "values".
    StructType(Seq(
      StructField("type", ByteType, nullable = false),
      StructField("size", IntegerType, nullable = true),
      StructField("indices", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("values", ArrayType(DoubleType, containsNull = false), nullable = true),
      StructField("labels", ArrayType(StringType,containsNull = false),nullable = true)
    ))
  }

  override def serialize(obj: LabeledVectorLike): GenericRowWithSchema =  {
    obj match {
      case LabeledSparseVector(size, indices, values, labels) =>
        new GenericRowWithSchema(Array(0.toByte,size,indices,values,labels),structType)

      case LabeledDenseVector(values, labels) =>
        new GenericRowWithSchema(Array(1.toByte,null,null,values,labels),structType)
    }
  }

  override def deserialize(datum: Any): LabeledVectorLike =  {
    datum match {
      case row: GenericRowWithSchema =>
        require(row.size == 5,
          s"VectorUDT.deserialize given row with length ${row.size} but requires length == 5")
        val tpe = row.getByte(0)
        tpe match {
          case 0 =>
            val size = row.getInt(1)
            val indices = row.getAs[Array[Int]](2)
            val values = row.getAs[Array[Double]](3)
            val labels = row.getAs[Array[String]](4)
            new LabeledSparseVector(size, indices, values,labels)
          case 1 =>
            val values = row.getAs[Array[Double]](3)
            val labels = row.getAs[Array[String]](4)
            new LabeledDenseVector(values,labels)
        }
    }
  }

  override def userClass: Class[LabeledVectorLike] = classOf[LabeledVectorLike]
}

object LabeledVectorUDT{
  val classNames = Seq( "grimoire.ml.linalg.LabeledDenseVector" , "grimoire.ml.linalg.LabeledSparseVector")
  val udtName = "org.apache.spark.grimoire.types.LabeledVectorUDT"

  def register():Unit = {
    classNames.foreach{
      case name =>
        if (! UDTRegistration.exists(name)) UDTRegistration.register(name,udtName)
    }
  }

  def isRegistered():Boolean = classNames.forall(UDTRegistration.exists(_))
}