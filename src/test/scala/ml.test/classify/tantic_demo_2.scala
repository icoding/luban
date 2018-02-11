package ml.test.classify

import grimoire.spark.globalSpark
import org.apache.spark.ml.feature.{Bucketizer, OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object tantic_demo_2 {
  globalSpark = SparkSession.
    builder().
    master("local[4]").
    appName("lr").
    enableHiveSupport().
    getOrCreate()


  val df = globalSpark.read.format("csv").option("header",true).option("inferSchema",true).load("file:///home/wuxiang/data/tantic/train.csv")

  val trainFeatures = df.withColumn("Surname", regexp_extract(col("Name"),"([\\w ']+),",1)).
    withColumn("Honorific", regexp_extract(col("name"),"(.*?)([\\w]+?)[.]",2)).
    withColumn("Mil", when(col("Honorific")==="Col" or col("Honorific") === "Major" or
      col("Honorific") === "Capt",1).otherwise(0)).
    withColumn("Doc", when(col("Honorific") === "Dr", 1).otherwise(0)).
    withColumn("Rev", when(col("Honorific") === "Rev", 1).otherwise(0)).
    withColumn("Nob", when(col("Honorific") === "Sir" or
      col("Honorific") === "Countess" or
      col("Honorific") === "Count" or
      col("Honorific") === "Duke" or
      col("Honorific") === "Duchess" or
      col("Honorific") === "Jonkheer" or
      col("Honorific") === "Don" or
      col("Honorific") === "Dona" or
      col("Honorific") === "Lord" or
      col("Honorific") === "Lady" or
      col("Honorific") === "Earl" or
      col("Honorific") === "Baron", 1).otherwise(0)).
    withColumn("Mr", when(col("Honorific") === "Mr", 1).otherwise(0)).
    withColumn("Mrs", when(col("Honorific") === "Mrs" or
      col("Honorific") === "Mme", 1).otherwise(0)).
    withColumn("Miss", when(col("Honorific") === "Miss" or
      col("Honorific") === "Mlle", 1).otherwise(0)).
    withColumn("Mstr", when(col("Honorific") === "Master", 1).otherwise(0)).
    withColumn("TotalFamSize",col("SibSp")+col("Parch")+1).
    withColumn("Singleton", when(col("TotalFamSize") === 1, 1).otherwise(0)).
    withColumn("SmallFam", when(col("TotalFamSize") <= 4 &&
      col("TotalFamSize") > 1, 1).otherwise(0)).
    withColumn("LargeFam", when(col("TotalFamSize") >= 5, 1).otherwise(0)).
    withColumn("Child", when(col("Age") <= 18, 1).otherwise(0)).
    withColumn("Mother", when(col("Age") > 15 &&
      col("Parch") > 0 &&
      col("Miss") === 0 &&
      col("Sex") === "female",1).otherwise(0))


//  (trainFeatures
//    .groupBy("Pclass","Embarked")
//    .agg(count("*"),avg("Fare"),min("Fare"),max("Fare"),stddev("Fare"))
//    .orderBy("Pclass","Embarked")
//    .show())

  val trainEmbarked = trainFeatures.na.fill("C",Seq("Embarked"))

  val trainMissDF = trainEmbarked.na.fill(22.0).where("Honorific = 'Miss'")
  val trainMasterDF = trainEmbarked.na.fill(5.0).where("Honorific = 'Master'")
  val trainMrDF = trainEmbarked.na.fill(32.0).where("Honorific = 'Mr'")
  val trainDrDF = trainEmbarked.na.fill(42.0).where("Honorific = 'Dr'")
  val trainMrsDF = trainEmbarked.na.fill(36.0).where("Honorific = 'Mrs'")

  val trainRemainderDF = globalSpark.sql("SELECT * FROM trainEmbarked WHERE Honorific NOT IN ('Miss','Master','Dr','Mr','Mrs')")

  val trainCombinedDF = trainRemainderDF.union(trainMissDF).union(trainMasterDF).union(trainMrDF).union(trainDrDF).union(trainMrsDF)


  val genderIndexer = new StringIndexer().setInputCol("Sex").setOutputCol("SexIndex")
  val embarkIndexer = new StringIndexer().setInputCol("Embarked").setOutputCol("EmbarkIndex")


  val genderEncoder = new OneHotEncoder().setInputCol("SexIndex").setOutputCol("SexVec")
  val embarkEncoder = new OneHotEncoder().setInputCol("EmbarkIndex").setOutputCol("EmbarkVec")


  val fareSplits = Array(0.0,10.0,20.0,30.0,40.0,60.0,120.0,Double.PositiveInfinity)
  val fareBucketize = new Bucketizer().setInputCol("Fare").setOutputCol("FareBucketed").setSplits(fareSplits)


  val assembler = new VectorAssembler().
    setInputCols(Array("Pclass", "SexVec", "Age", "SibSp", "Parch", "Fare", "FareBucketed", "EmbarkVec", "Mil", "Doc", "Rev", "Nob", "Mr", "Mrs", "Miss", "Mstr", "TotalFamSize", "Singleton", "SmallFam", "LargeFam", "Child", "Mother")).
    setOutputCol("features")
}
