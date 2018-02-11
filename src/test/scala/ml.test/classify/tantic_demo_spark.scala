package ml.test.classify

import grimoire.spark.globalSpark
import org.apache.spark.ml.classification.{RandomForestClassifier,RandomForestClassificationModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object tantic_demo_spark {
  globalSpark = SparkSession.
    builder().
    master("local[4]").
    appName("lr").
    enableHiveSupport().
    getOrCreate()

  val schemaArray = Array(
    StructField("PassengerId", IntegerType, true),
    StructField("Survived", IntegerType, true),
    StructField("Pclass", IntegerType, true),
    StructField("Name", StringType, true),
    StructField("Sex", StringType, true),
    StructField("Age", FloatType, true),
    StructField("SibSp", IntegerType, true),
    StructField("Parch", IntegerType, true),
    StructField("Ticket", StringType, true),
    StructField("Fare", FloatType, true),
    StructField("Cabin", StringType, true),
    StructField("Embarked", StringType, true)
  )
  val trainSchema = StructType(schemaArray)

  val df = globalSpark.read.schema(trainSchema).option("header", "true").
    csv("file:///home/wuxiang/data/tantic/train.csv")


//  val Pattern = ".*, (.*?)\\..*".r
//  val title: (String => String) = {
//    case Pattern(t) => t
//    case _ => ""
//  }
//  val titleUDF = udf(title)
//  val dfWithTitle = df.withColumn("Title", titleUDF(col("Name")))

  val Pattern = ".*, (.*?)\\..*".r
  val titles = Map(
    "Mrs"    -> "Mrs",
    "Lady"   -> "Mrs",
    "Mme"    -> "Mrs",
    "Ms"     -> "Ms",
    "Miss"   -> "Miss",
    "Mlle"   -> "Miss",
    "Master" -> "Master",
    "Rev"    -> "Rev",
    "Don"    -> "Mr",
    "Sir"    -> "Sir",
    "Dr"     -> "Dr",
    "Col"    -> "Col",
    "Capt"   -> "Col",
    "Major"  -> "Col"
  )

  val title: ((String, String) => String) = {
    case (Pattern(t), sex) => titles.get(t) match {
      case Some(tt) => tt
      case None     =>
        if (sex == "male") "Mr"
        else "Mrs"
    }
    case _ => "Mr"
  }
  val titleUDF = udf(title)

  val dfWithTitle = df.withColumn("Title", titleUDF(col("Name"), col("Sex")))



  val familySize: ((Int, Int) => Int) = (sibSp: Int, parCh: Int) => sibSp + parCh + 1
  val familySizeUDF = udf(familySize)
  val dfWithFamilySize = dfWithTitle.withColumn("FamilySize", familySizeUDF(col("SibSp"), col("Parch")))


  val avg_age = dfWithFamilySize.agg("Age"->"avg").rdd.collect match {
    case Array(Row(avg:Double)) => avg
    case _ => 0
  }
  val avg_Fare = dfWithFamilySize.agg("Fare"->"avg").rdd.collect match {
    case Array(Row(avg:Double)) => avg
    case _ => 0
  }
  val df1=dfWithFamilySize.na.fill(Map("Age"->avg_age,"Fare" ->avg_Fare,"Embarked"->"S"))


  val categoricalFeatColNames = Seq("Pclass", "Sex", "Embarked", "Title")
  val stringIndexers = categoricalFeatColNames.map { colName =>
    new StringIndexer().setInputCol(colName).
      setOutputCol(colName + "Indexed").fit(df1)
  }

  val labelIndexer = new StringIndexer().setInputCol("Survived").
    setOutputCol("SurvivedIndexed").fit(df1)



  val numericFeatColNames = Seq("Age", "SibSp", "Parch", "Fare", "FamilySize")
  val idxdCategoricalFeatColName = categoricalFeatColNames.map(_ + "Indexed")
  val allIdxdFeatColNames = numericFeatColNames ++ idxdCategoricalFeatColName
  val assembler = new VectorAssembler().
    setInputCols(Array(allIdxdFeatColNames: _*)).
    setOutputCol("Features")


  val randomForest = new RandomForestClassifier().
    setLabelCol("SurvivedIndexed").setFeaturesCol("Features")

  val labelConverter = new IndexToString().
    setInputCol("prediction").
    setOutputCol("predictedLabel").
    setLabels(labelIndexer.labels)



  val pipeline = new Pipeline().setStages(stringIndexers.toArray++
    Array(labelIndexer, assembler, labelConverter))



  val paramGrid = new ParamGridBuilder().
    addGrid(randomForest.maxBins, Array(25, 28, 31)).
    addGrid(randomForest.maxDepth, Array(4, 6, 8)).
    addGrid(randomForest.impurity, Array("entropy", "gini")).
    build()


  val evaluator = new BinaryClassificationEvaluator().
    setLabelCol("SurvivedIndexed").setMetricName("areaUnderPR")

  val cv = new CrossValidator().setEstimator(pipeline).
    setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(10)


  val crossValidatorModel = cv.fit(df1)
}
