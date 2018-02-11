package ml.test.classify

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.{SparkConf, SparkContext}

object BinaryClassificationMetrics_demo_test {
  val conf = new SparkConf().setAppName("test").setMaster("local") // 调试的时候一定不要用local[*]
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val TP = Array((1.0,1.0))

  val TN = new Array[(Double,Double)](89)
  for(i<-TN.indices){
    TN(i) = (0.0,0.0)
  }

  val FN1 = new Array[(Double,Double)](5)
  for(i<-FN1.indices){
    FN1(i) = (0.6,1.0)
  }
  val FN2 = new Array[(Double,Double)](5)
  for(i<-FN2.indices){
    FN2(i) = (0.4,1.0)
  }

  val all = TP ++ TN ++ FN1 ++ FN2
//  val scoreAndLabels = sc.parallelize(all)
  val scoreAndLabels = sc.parallelize(all, 2)

  val metrics = new BinaryClassificationMetrics(scoreAndLabels)

  val binnedCounts = scoreAndLabels.combineByKey(
    createCombiner = (label: Double) => new BinaryLabelCounter(0L, 0L) += label,
    mergeValue = (c: BinaryLabelCounter, label: Double) => c += label,
    mergeCombiners = (c1: BinaryLabelCounter, c2: BinaryLabelCounter) => c1 += c2
  ).sortByKey(ascending = false)

}
