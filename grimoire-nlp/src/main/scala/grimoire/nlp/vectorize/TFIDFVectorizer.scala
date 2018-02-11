package grimoire.nlp.vectorize

import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame

/**
  * Created by caphael on 2016/10/28.
  */
class TfidfVectorizer() extends DFVectorizer{

//  private val defNormFunc :(Vector)=>(Vector) = (v)=>{v}

  private var normFunc :(Vector)=>(Vector) = null
  private var idfModel : IDFModel = null

  private var inputCol:String="words"
  private var tfCol:String = "tf"
  private var tfidfCol:String = "tfidf"


  def setNormFunc(func:(Vector)=>(Vector)):TfidfVectorizer = {
    this.normFunc = func
    this
  }

  def setModel(model:IDFModel):TfidfVectorizer = {
    this.idfModel = model
    this
  }

  def setInputCol(name:String):TfidfVectorizer ={
    this.inputCol = name
    this
  }
  def setTfCol(name:String):TfidfVectorizer ={
    this.tfCol = name
    this
  }

  def setTfidfCol(name:String):TfidfVectorizer = {
    this.tfidfCol = name
    this
  }

  def vectorize(data:DataFrame
               ):DataFrame={

    val tfVectorizer = new TfVectorizer().setInputCol(inputCol).setOutputCol(tfCol).setNormFunc(normFunc)

    val tfNorm = tfVectorizer.vectorize(data)

    //If model is null, train new one
    if ( this.idfModel == null) {
      this.idfModel = new IDF().setMinDocFreq(1).setInputCol(tfCol+"Norm").setOutputCol(tfidfCol).fit(tfNorm)
    }

    idfModel.transform(tfNorm)
  }

}

object TfidfVectorizer {
  def apply():TfidfVectorizer = {
    return new TfidfVectorizer()
  }
}
