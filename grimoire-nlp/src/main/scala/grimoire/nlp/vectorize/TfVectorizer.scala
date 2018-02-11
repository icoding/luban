package grimoire.nlp.vectorize

import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by caphael on 2016/10/31.
  */
class TfVectorizer extends DFVectorizer{

//  private val defNormFunc :(Vector)=>(Vector) = (v)=>{v}

  private var normFunc :(Vector)=>(Vector) = null

  private var inputCol:String="words"
  private var outputCol:String = "tf"


  def setNormFunc(func:(Vector)=>(Vector)):TfVectorizer = {
    this.normFunc = func
    this
  }

  def setInputCol(name:String):TfVectorizer ={
    this.inputCol = name
    this
  }
  def setOutputCol(name:String):TfVectorizer ={
    this.outputCol = name
    this
  }

  def vectorize(data:DataFrame
               ):DataFrame={
    val outputRawCol:String = if(normFunc == null ){outputCol}else{outputCol+"RAW"}
    val hashingTF = new HashingTF().setInputCol(inputCol).setOutputCol(outputRawCol)

    // Filter Empty Vectors
    val notEmpty:(Vector) => (Boolean) = (v:Vector) => {v.numActives>0}
    val udfNotEmpty = udf(notEmpty)

    val tfAll = hashingTF.transform(data)

    val tfActive = tfAll.filter(udfNotEmpty(tfAll.col(outputRawCol)))

    //If normFunc is null, Don't normalize
    if(normFunc == null){
      tfActive
    }else{
      val udfNormFun = udf(normFunc)
      tfActive.withColumn(outputCol,udfNormFun(tfActive.col(outputRawCol)))
    }
  }
}