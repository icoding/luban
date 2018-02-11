package grimoire.nlp.vectorize

import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors => MLBVectors}
import org.apache.spark.mllib.linalg.{Vector => MLLIBVector, Vectors => MLLIBVectors}
import org.apache.spark.sql.DataFrame

/**
  * Created by caphael on 2016/10/31.
  */
abstract class DFVectorizer {

  private[this] var toLibV:Boolean = false

  def vectorize(data:DataFrame):DataFrame

}
