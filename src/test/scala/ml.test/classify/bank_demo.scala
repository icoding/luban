package ml.test.classify

import grimoire.spark.globalSpark
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object bank_demo {
    globalSpark = SparkSession.
      builder().
      master("local[4]").
      appName("bank_data_demo").
      enableHiveSupport().
      getOrCreate()

  val bank_Marketing_Data=globalSpark.read.option("header", true).
    option("inferSchema", "true").csv("file:///home/wuxiang/data/bank_marketing_data.csv")

  val selected_Data=bank_Marketing_Data.select("age",
    "job",
    "marital",
    "default",
    "housing",
    "loan",
    "duration",
    "previous",
    "poutcome",
    "empvarrate",
    "y").
    withColumn("age", bank_Marketing_Data("age").cast(DoubleType)).
    withColumn("duration", bank_Marketing_Data("duration").cast(DoubleType)).
    withColumn("previous", bank_Marketing_Data("previous").cast(DoubleType))

  val summary=selected_Data.describe()
  summary.show()


  val columnNames=selected_Data.columns
  val uniqueValues_PerField=columnNames.map { field => field+":"+selected_Data.select(field).distinct().count() }

  val indexer = new StringIndexer().setInputCol("job").setOutputCol("jobIndex")
  val indexed = indexer.fit(selected_Data).transform(selected_Data)


  val encoder = new OneHotEncoder().setDropLast(false).setInputCol("jobIndex").setOutputCol("jobVec")
  val encoded = encoder.transform(indexed)

  val maritalIndexer=new StringIndexer().setInputCol("marital").setOutputCol("maritalIndex")
  val maritalIndexed=maritalIndexer.fit(encoded).transform(encoded)
  val maritalEncoder=new OneHotEncoder().setDropLast(false).setInputCol("maritalIndex").setOutputCol("maritalVec")
  val maritalEncoded=maritalEncoder.transform(maritalIndexed)

  val defaultIndexer=new StringIndexer().setInputCol("default").setOutputCol("defaultIndex")
  val defaultIndexed=defaultIndexer.fit(maritalEncoded).transform(maritalEncoded)
  val defaultEncoder=new OneHotEncoder().setDropLast(false).setInputCol("defaultIndex").setOutputCol("defaultVec")
  val defaultEncoded=defaultEncoder.transform(defaultIndexed)

  val housingIndexer=new StringIndexer().setInputCol("housing").setOutputCol("housingIndex")
  val housingIndexed=housingIndexer.fit(defaultEncoded).transform(defaultEncoded)
  val housingEncoder=new OneHotEncoder().setDropLast(false).setInputCol("housingIndex").setOutputCol("housingVec")
  val housingEncoded=housingEncoder.transform(housingIndexed)

  val poutcomeIndexer=new StringIndexer().setInputCol("poutcome").setOutputCol("poutcomeIndex")
  val poutcomeIndexed=poutcomeIndexer.fit(housingEncoded).transform(housingEncoded)
  val poutcomeEncoder=new OneHotEncoder().setDropLast(false).setInputCol("poutcomeIndex").setOutputCol("poutcomeVec")
  val poutcomeEncoded=poutcomeEncoder.transform(poutcomeIndexed)

  val loanIndexer=new StringIndexer().setInputCol("loan").setOutputCol("loanIndex")
  val loanIndexed=loanIndexer.fit(poutcomeEncoded).transform(poutcomeEncoded)
  val loanEncoder=new OneHotEncoder().setDropLast(false).setInputCol("loanIndex").setOutputCol("loanVec")
  val loanEncoded=loanEncoder.transform(loanIndexed)

  val vectorAssembler = new VectorAssembler().
    setInputCols(Array("jobVec","maritalVec", "defaultVec","housingVec","poutcomeVec","loanVec","age","duration","previous","empvarrate")).
    setOutputCol("features")


  val indexerY = new StringIndexer().setInputCol("y").setOutputCol("label")


  val transformers=Array(indexer,
    encoder,
    maritalIndexer,
    maritalEncoder,
    defaultIndexer,
    defaultEncoder,
    housingIndexer,
    housingEncoder,
    poutcomeIndexer,
    poutcomeEncoder,
    loanIndexer,
    loanEncoder,
    vectorAssembler,
    indexerY)

  val splits = selected_Data.randomSplit(Array(0.8,0.2))
  val training = splits(0).cache()
  val test = splits(1).cache()

  val lr = new LogisticRegression()
  var model = new Pipeline().setStages(transformers :+ lr).fit(training)
  var result = model.transform(test)
  val evaluator = new BinaryClassificationEvaluator()
  var aucTraining = evaluator.evaluate(result)
  println("aucTraining = "+aucTraining)



  val dt = result.select("prediction","label").rdd.map{
    case Row(a:Double,b:Double) =>
      (a,b)
    case Row(a:Vector,b:Double) =>
      (Row(a:Vector,b:Double).getAs[Vector](0)(1), Row(a:Vector,b:Double).getAs[Double](1))
  }
  val bcm = new BinaryClassificationMetrics(dt)
}
