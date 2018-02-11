package grimoire.ml.configuration.param

import com.sun.xml.internal.fastinfoset.util.StringArray
import grimoire.configuration.ConfigLike
import grimoire.configuration.param.{Param, ParamValidators}
import grimoire.configuration.JsonFormatFactory._
import org.apache.spark.ml.{Pipeline, PipelineStage}
import play.api.libs.json.{Format, JsPath}


/**
  * Created by caphael on 2017/3/22.
  */
trait HasK extends ConfigLike{
  final val k:Param[Int] = new Param[Int](this,"K","The number of clusters to infer. Must be > 1.",ParamValidators.gt(1)) {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }.required().default(4)

  def setK(a:Option[Int]):this.type = set(k,a)
}

trait HasSeed extends ConfigLike {
  final val seed:Param[Long] = new Param[Long](this, "Seed", "Random seed.") {
    @transient override val format: Format[Option[Long]] = FORMAT.LONG
  }.default(159147643)

  def setSeed(a:Option[Long]):this.type = set(seed,a)
}

trait HasIsLocalModel extends ConfigLike {
  final val isLocalModel: Param[Boolean] = new Param[Boolean](this, "IsLocalModel", "Whether it is a local model.") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(true)

  def setIsLocalModel(a:Option[Boolean]):this.type = set(isLocalModel,a)
}

trait HasLabelCol extends ConfigLike{
  final val labelCol = new Param[String](this,"LabelCol","Column name of label.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("label").required()

  def setLabelCol(a:Option[String]):this.type = set(labelCol,a)
}

trait HasIndexedlabelCol extends ConfigLike{
  final val indexedlabelCol = new Param[String](this,"IndexedlabelCol","Column name of label.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("indexedlabel").required()

  def setIndexedlabelCol(a:Option[String]):this.type = set(indexedlabelCol,a)
}

trait HasCensorCol extends ConfigLike{
  final val censorCol = new Param[String](this,"censorCol","Param for censor column name.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("label")

  def setCensorCol(a:Option[String]):this.type = set(censorCol,a)
}

trait HasFeaturesCol extends ConfigLike{
  final val featuresCol = new Param[String](this,"FeaturesCol","Column of features.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("features").required()

  def setFeaturesCol(a:Option[String]):this.type = set(featuresCol,a)
}

trait HasFeatureCols extends ConfigLike{
  final val featureCols = new Param[Seq[String]](this,"FeatureCols","Columns of features.") {
    @transient override val format: Format[Option[Seq[String]]] = FORMAT.SEQ.STRING
  }.default(Seq("*"))

  def setFeatureCols(a:Option[Seq[String]]):this.type = set(featureCols,a)
}

trait HasBiMapKeeper extends ConfigLike {
  final val biMapKeeper = new Param[Boolean](this,"BiMapKeeper","Whether use BiMapKeeper.") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(false)

  def setBiMapKeeper(a:Option[Boolean]):this.type = set(biMapKeeper,a)
}

trait HasStartFromZero extends ConfigLike {
  final val startFromZero = new Param[Boolean](this,"StartFromZero","Index start from 0(true) or 1(false).") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(true)

  def setStartFromZero(a:Option[Boolean]):this.type  = set(startFromZero,a)
}

trait HasStepSize extends ConfigLike {
  final val stepSize = new Param[Double](this,"StepSize","Set the initial step size of SGD for the first step. Default 1.0. In subsequent steps, the step size will decrease with stepSize/sqrt(t)",ParamValidators.gt(0)) {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(1.0)

  def setStepSize(a:Option[Double]):this.type = set(stepSize,a)
}

trait HasNumIter extends ConfigLike {
  final val numIter = new Param[Int](this,"NumIter","Set the number of iterations for SGD. Default 100.",ParamValidators.gt(0)) {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }.default(100)

  def setNumIter(a:Option[Int]) : this.type = set(numIter,a)
}

trait HasMiniBatchFrac extends ConfigLike {
  final val miniBatchFrac = new Param[Double](this,"MiniBatchFrac","Set fraction of data to be used for each SGD iteration. Default 1.0 (corresponding to deterministic/classical gradient descent)") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(1.0)

  def setMiniBatchFrac(a:Option[Double]):this.type = set(miniBatchFrac,a)
}

trait HasBreaks extends ConfigLike{
  final val breaks = new Param[Seq[Double]](this,"Breaks","The number of intervals into which the data is to be cut") {
    override val format: Format[Option[Seq[Double]]] = FORMAT.SEQ.DOUBLE
  }

  def setBreaks(a:Option[Seq[Double]]):this.type = set(breaks,a)
}

trait HasLabels extends ConfigLike{
  final val labels = new Param[Seq[String]](this,"Labels","Labels for the resulting category. default: (lower,upper].") {
    @transient override val format: Format[Option[Seq[String]]] = FORMAT.SEQ.STRING
  }.default(Seq[String]())

  def setLabels(a:Option[Seq[String]]):this.type = set(labels,a)
}

trait HasRightClose extends ConfigLike{
  final val rightClose = new Param[Boolean](this,"RightClose","Indicating if the intervals should be closed on the right (and open on the left). default: true.") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(true)

  def setRightClose(a:Option[Boolean]=Some(true)):this.type = set(rightClose,Some(true))
  def setLeftClose():this.type = set(rightClose,Some(false))
}

trait HasReverseKeeper extends ConfigLike{
  final val reverseKeeper = new Param[Boolean](this,"ReverseKeeper","Whether reverse the keeper") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(false)

  def setReverseKeeper(a:Option[Boolean]):this.type = set(reverseKeeper,a)
}

trait HasThreshold extends ConfigLike{
  final val threshold = new Param[Double](this,"Threshold","Limit of calculation") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(0.5)

  def setThreshold(a:Option[Double]):this.type = set(threshold,a)
}

trait HasThresholds extends ConfigLike{
  final val thresholds = new Param[Seq[Double]](this,"Thresholds","Param for Thresholds in multi-class classification to adjust the probability of predicting each class.") {
    @transient override val format: Format[Option[Seq[Double]]] = FORMAT.SEQ.DOUBLE
  }

  def setThresholds(a:Option[Seq[Double]]):this.type = set(thresholds,a)
}

trait HasVectorSize extends ConfigLike{
  final val vectorsize = new Param[Int](this,"vectorsize","The size of vector") {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }.required()

  def setVectorSize(a:Option[Int]):this.type = set(vectorsize,a)
}

trait HasMinCount extends ConfigLike{
  final val mincount = new Param[Int](this,"MinCount","The min number of counting") {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }.required()

  def setMinCount(a:Option[Int]):this.type = set(mincount,a)
}

trait HasVocabSize extends ConfigLike{
  final val vocabSize = new Param[Int](this,"VocabSize","Max size of the vocabulary.") {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }

  def setVocabSize(a:Option[Int]):this.type = set(vocabSize,a)
}

trait HasMinDF extends ConfigLike{
  final val minDF = new Param[Double](this,"MinDF","Specifies the minimum number of different documents a term must appear in to be included in the vocabulary.") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }

  def setMinDF(a:Option[Double]):this.type = set(minDF,a)
}

trait HasMinTF extends ConfigLike{
  final val minTF = new Param[Double](this,"minTF","Filter to ignore rare words in a document.") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(0.2)

  def setMinTF(a:Option[Double]):this.type = set(minTF,a)
}

trait HasN extends ConfigLike{
  final val n = new Param[Int](this,"N","Minimum n-gram length, greater than or equal to 1.") {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }.required()

  def setN(a:Option[Int]):this.type = set(n,a)
}

trait HasPattern extends ConfigLike{
  final val pattern = new Param[String](this,"Pattern","Regex pattern used to match delimiters if gaps is true or tokens if gaps is false.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }

  def setPattern(a:Option[String]):this.type = set(pattern,a)
}

trait HasGaps extends ConfigLike{
  final val gaps = new Param[Boolean](this,"gaps","Indicates whether regex splits on gaps (true) or matches tokens (false)..") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.required()

  def setGaps(a:Option[Boolean]):this.type = set(gaps,a)
}

trait HasMintokenLength extends ConfigLike{
  final val minTokenLength = new Param[Int](this,"MinTokenLength","Minimum token length.") {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }

  def setMintokenLength(a:Option[Int]):this.type = set(minTokenLength,a)
}

trait HasToLowercase extends ConfigLike{
  final val toLowercase = new Param[Boolean](this,"ToLowercase","Indicates whether to convert all characters to lowercase before tokenizing.") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }

  def setToLowercase(a:Option[Boolean]):this.type = set(toLowercase,a)
}

trait HasDegree extends ConfigLike{
  final val degree = new Param[Int](this,"degree","The polynomial degree to expand.") {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }.required()

  def setDegree(a:Option[Int]):this.type = set(degree,a)
}

trait HasDividedNumber extends ConfigLike{
  final val dividedN = new Param[Int](this,"DividedNumber","The range of samples to divide into N pieces.") {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }.default(100)

  def setDividedNumber(n:Option[Int]):this.type = set(dividedN,n)
}

trait HasInverse extends ConfigLike{
  final val inverse = new Param[Boolean](this,"Inverse"," Indicates whether to perform the inverse DCT (true) or forward DCT (false).") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.required()

  def setInverse(a:Option[Boolean]):this.type = set(inverse,a)
}

trait HasMaxCategories extends ConfigLike{
  final val maxCategories = new Param[Int](this,"MaxCategories","Threshold for the number of values a categorical feature can take.") {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }.required()

  def setMaxCategories(a:Option[Int]):this.type = set(maxCategories,a)
}

trait HasInputCols extends ConfigLike{
  final val inputCols = new Param[Seq[String]](this,"InputCols","Param for input column names.") {
    @transient override val format: Format[Option[Seq[String]]] = FORMAT.SEQ.STRING
  }.required()

  def setInputCols(a:Option[Seq[String]]):this.type = set(inputCols,a)
}

trait HasP extends ConfigLike{
  final val p = new Param[Double](this,"P","Normalization in Lp space.") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.required().default(2)

  def setP(a:Option[Double]):this.type = set(p,a)
}

trait HasSplits extends ConfigLike{
  final val splits = new Param[Seq[Double]](this,"Splits","Parameter for mapping continuous features into buckets.") {
    @transient override val format: Format[Option[Seq[Double]]] = FORMAT.SEQ.DOUBLE
  }.required()

  def setSplits(a:Option[Seq[Double]]):this.type = set(splits,a)
}

trait HasNumTopFeatures extends ConfigLike{
  final val numTopFeatures = new Param[Int](this,"NumTopFeatures","Number of features that selector will select, ordered by ascending p-value.") {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }.required()

  def setNumTopFeatures(a:Option[Int]):this.type = set(numTopFeatures,a)
}

trait HasStatement extends ConfigLike{
  final val statement = new Param[String](this,"statement","SQL statement parameter.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.required()

  def setStatement(a:Option[String]):this.type = set(statement,a)
}

trait HasNumBuckets extends ConfigLike{
  final val numBuckets = new Param[Int](this,"numBuckets","Number of buckets (quantiles, or categories) into which data points are grouped.") {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }.required()

  def setNumBuckets(a:Option[Int]):this.type = set(numBuckets,a)
}

trait HasHandleInvalid extends ConfigLike{
  final val handleInvalid = new Param[String](this,"HandleInvalid","Param for how to handle invalid entries.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }

  def setHandleInvalid(a:Option[String]):this.type = set(handleInvalid,a)
}

trait HasRelativeError extends ConfigLike{
  final val relativeError = new Param[Double](this,"relativeError","Relative error.") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.required()

  def setRelativeError(a:Option[Double]):this.type = set(relativeError,a)
}

trait HasFormula extends ConfigLike{
  final val formula = new Param[String](this,"formula","Param for features column name.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.required()

  def setFormula(a:Option[String]):this.type = set(formula,a)
}

trait HasBucketLength extends ConfigLike{
  final val bucketLength = new Param[Double](this,"bucketLength","The length of each hash bucket, a larger bucket lowers the false negative rate.") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.required()

  def setBucketLength(a:Option[Double]):this.type = set(bucketLength,a)
}

trait HasCorrelationMethod extends ConfigLike{
  final val corMethod = new Param[String](this,"CorrelationMethod","Correlation method(default: pearson; alternative: spearman).") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("pearson")

  def setCorrelationMethod(m:Option[String]):this.type = set(corMethod,m)
}


trait HasNumHashTables extends ConfigLike{
  final val numHashTables = new Param[Int](this,"numHashTables","Param for the number of hash tables used in LSH OR-amplification.") {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }.required()

  def setNumHashTables(a:Option[Int]):this.type = set(numHashTables,a)
}

trait HasIndices extends ConfigLike{
  final val indices = new Param[Seq[Int]](this,"indices","An array of indices to select features from a vector column.") {
    override val format: Format[Option[Seq[Int]]] = FORMAT.SEQ.INT
  }

  def setIndices(a:Option[Seq[Int]]):this.type = set(indices,a)
}

trait HasNames extends ConfigLike{
  final val names = new Param[Seq[String]](this,"names","An array of feature names to select features from a vector column.") {
    override val format: Format[Option[Seq[String]]] = FORMAT.SEQ.STRING
  }

  def setNames(a:Option[Seq[String]]):this.type = set(names,a)
}

trait HasScalingVec extends ConfigLike{
  final val scalingVec = new Param[Seq[Double]](this,"scalingVec","The vector to multiply with input vectors.") {
    override val format: Format[Option[Seq[Double]]] = FORMAT.SEQ.DOUBLE
  }

  def setScalingVec(a:Option[Seq[Double]]):this.type = set(scalingVec,a)
}

trait HasElasticNetParam extends ConfigLike{
  final val elasticNetParam = new Param[Double](this,"ElasticNetParam","The param of α.") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(0.0)

  def setElasticNetParam(a:Option[Double]):this.type = set(elasticNetParam,a)
}

trait HasEstimatedPoints extends ConfigLike{
  final val estimatedPoints = new Param[Seq[Double]](this,"EstimatedPoints","Points to estimate density.") {
    @transient override val format: Format[Option[Seq[Double]]] = FORMAT.SEQ.DOUBLE
  }.default(Seq[Double]())

  def setEstimatedPoints(points:Option[Seq[Double]]):this.type = set(estimatedPoints,points)
}

trait HasFamily extends ConfigLike{
  final val family = new Param[String](this,"Family","binomial logistic regression or multinomial logistic regression") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("auto")

  def setFamily(a:Option[String]):this.type = set(family,a)
}

trait HasPredictionCol extends ConfigLike{
  final val predictionCol = new Param[String](this,"PredictionCol","Param for prediction column name.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("prediction").required()

  def setPredictionCol(a:Option[String]):this.type = set(predictionCol,a)
}

trait HasOriginlabelCol extends ConfigLike{
  final val originlabelCol = new Param[String](this,"OriginlabelCol","Param for originlabelCol column name.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }

  def setOriginlabelCol(a:Option[String]):this.type = set(originlabelCol,a)
}


trait HasRawPredictionCol extends ConfigLike{
  final val rawPredictionCol = new Param[String](this,"RawPredictionCol","Param for prediction column name.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("rawPrediction")

  def setRawPredictionCol(a:Option[String]):this.type = set(rawPredictionCol,a)
}

trait HasLinkPredictionCol extends ConfigLike{
  final val linkPredictionCol = new Param[String](this,"linkPredictionCol","Param for link prediction (linear predictor) column name.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }

  def setLinkPredictionCol(a:Option[String]):this.type = set(linkPredictionCol,a)
}

trait HasVarianceCol extends ConfigLike{
  final val varianceCol = new Param[String](this,"varianceCol","Param for Column name for the biased sample variance of prediction.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }

  def setVarianceCol(a:Option[String]):this.type = set(varianceCol,a)
}

trait HasProbabilityCol extends ConfigLike{
  final val probabilityCol = new Param[String](this,"ProbabilityCol","Param for Column name for predicted class conditional probabilities.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("probabilities")

  def setProbabilityCol(a:Option[String]):this.type = set(probabilityCol,a)
}

trait HasWeightCol extends ConfigLike{
  final val weightCol = new Param[String](this,"WeightCol","Param for weight column name.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }

  def setWeightCol(a:Option[String]):this.type = set(weightCol,a)
}

trait HasMetricName extends ConfigLike{
  final val metricName = new Param[String](this,"MetricName","Param of evaluate.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.required()

  def setMetricName(a:Option[String]):this.type = set(metricName,a)
}

trait HasNumTrees extends ConfigLike{
  final val numTrees = new Param[Int](this,"NumTrees","Number of trees to train.") {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }.required().default(10)

  def setNumTrees(a:Option[Int]):this.type = set(numTrees,a)
}


trait HasLayers extends ConfigLike{
  final val layers = new Param[Seq[Int]](this,"layers","Layer sizes including input size and output size.") {
    override val format: Format[Option[Seq[Int]]] = FORMAT.SEQ.INT
  }

  def setLayers(a:Option[Seq[Int]]):this.type = set(layers,a)
}

trait HasBlockSize extends ConfigLike{
  final val blockSize = new Param[Int](this,"blockSize","size of block.") {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }.required()

  def setBlockSize(a:Option[Int]):this.type = set(blockSize,a)
}

trait HasUserCol extends ConfigLike{
  final val userCol = new Param[String](this,"UserCol","Param for the column name for user ids.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.required()

  def setUserCol(a:Option[String]):this.type = set(userCol,a)
}

trait HasItemCol extends ConfigLike{
  final val itemCol = new Param[String](this,"ItemCol","Param for the column name for item ids.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.required()

  def setItemCol(a:Option[String]):this.type = set(itemCol,a)
}

trait HasRatingCol extends ConfigLike{
  final val ratingCol = new Param[String](this,"RatingCol","Param for the column name for ratings.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.required()

  def setRatingCol(a:Option[String]):this.type = set(ratingCol,a)
}

trait HasMinSupport extends ConfigLike{
  final val minSupport = new Param[Double](this,"minSupport"," the minimum support for an itemset to be identified as frequent.") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(0.5)

  def setMinSupport(a:Option[Double]):this.type = set(minSupport,a)
}

trait HasNumPartitions extends ConfigLike{
  final val numPartitions = new Param[Int](this,"numPartitions","the number of partitions used to distribute the work.") {
    @transient override val format: Format[Option[Int]] = FORMAT.INT
  }.required()

  def setNumPartitions(a:Option[Int]):this.type = set(numPartitions,a)
}

trait HasLink extends ConfigLike{
  final val link = new Param[String](this,"link","link function") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.required()

  def setLink(a:Option[String]):this.type = set(link,a)
}

trait TrainSet extends ConfigLike{
  final val trainSet = new Param[String](this,"Trainset","split dataset to trainset") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("trainset")

  def setTrainSet(a:Option[String]):this.type = set(trainSet,a)
}


trait TestSet extends ConfigLike{
  final val testSet = new Param[String](this,"TestSet","split dataset to testset") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("testset")

  def setTestSet(a:Option[String]):this.type = set(testSet,a)
}

trait HasRandomRate extends ConfigLike{
  final val randomRate = new Param[Double](this,"RandomRate","trainset random rate") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.required()

  def setRandomRate(a:Option[Double]):this.type = set(randomRate,a)
}
trait HasTakeTrain extends ConfigLike{
  final val taketrain = new Param[Boolean](this,"takeTrain","take trainset or testset") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(true)

  def setTakeTrain(a:Option[Boolean]):this.type = set(taketrain,a)
}

trait HasQuantileProbabilities extends ConfigLike{
  final val quantileProbabilities = new Param[Seq[Double]](this,"quantileProbabilities","quantile probabilities") {
    @transient override val format: Format[Option[Seq[Double]]] = FORMAT.SEQ.DOUBLE
  }.required()

  def setQuantileProbabilities(a:Option[Seq[Double]]):this.type = set(quantileProbabilities,a)
}
trait HasQuantilesCol extends ConfigLike{
  final val quantilesCol = new Param[String](this,"quantilesCol","quantiles column") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.required()

  def setQuantilesCol(a:Option[String]):this.type = set(quantilesCol,a)
}

trait HasNumClasses extends ConfigLike{
  final val numClasses = new Param[Int](this,"numClasses","the  number of classes ") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }

  def setNumClasses(a:Option[Int]):this.type = set(numClasses,a)
}
trait HasNumIterations extends ConfigLike{
  final val numIterations = new Param[Int](this,"NumIterations","the  number of iterations ") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(10)

  def setNumIterations(a:Option[Int]):this.type = set(numIterations,a)
}

trait HasNumRecommend extends ConfigLike{
  final val numRecommend = new Param[Int](this,"NumRecommend","the  number of recommend value ") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(100)

  def setNumRecommend(a:Option[Int]):this.type = set(numRecommend,a)
}

trait HasMaxIterations extends ConfigLike{
  final val maxIterations = new Param[Int](this,"maxIterations","the  maximum of iterations ") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }

  def setMaxIterations(a:Option[Int]):this.type = set(maxIterations,a)
}

trait HasMaxIter extends ConfigLike{
  final val maxIter = new Param[Int](this,"maxIter","Param for maximum number of iterations ") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }

  def setMaxIter(a:Option[Int]):this.type = set(maxIter,a)
}

trait HasInitializationMode extends ConfigLike{
  final val initializationMode = new Param[String](this,"initializationMode","prediction column") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.required()

  def setInitializationMode(a:Option[String]):this.type = set(initializationMode,a)
}

trait HasMinConfidence extends ConfigLike{
  final val minConfidence = new Param[Double](this,"minConfidence","the  min of confidence ") {
    override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(0.8)

  def setMinConfidence(a:Option[Double]):this.type = set(minConfidence,a)
}

trait HasMaxPatternLength extends ConfigLike{
  final val maxPatternLength = new Param[Int](this,"maxPatternLength","the  maximum length of pattern ") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(5)

  def setMaxPatternLength(a:Option[Int]):this.type = set(maxPatternLength,a)
}

trait HasNumFolds extends ConfigLike{
  final val numFolds = new Param[Int](this,"numFolds","Param for number of folds for cross validation. ") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(2)

  def setNumFolds(a:Option[Int]):this.type = set(numFolds,a)
}

trait HasNumFeatures extends ConfigLike{
  final val numFeatures = new Param[Int](this,"NumFeatures","Param for number of feature. ") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(20)

  def setNumFeatures(a:Option[Int]):this.type = set(numFeatures,a)
}

trait HasTrainRatio extends ConfigLike{
  final val trainRatio = new Param[Double](this,"trainRatio","the  ratio of train ") {
  override val format: Format[Option[Double]] = FORMAT.DOUBLE
}.default(0.8)

  def setTrainRatio(a:Option[Double]):this.type = set(trainRatio,a)
}

trait HasIsotonic extends ConfigLike {
  final val isotonic = new Param[Boolean](this,"isotonic","Param for whether the output sequence should be isotonic/increasing (true) or antitonic/decreasing (false).") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(true)

  def setIsotonic(a:Option[Boolean]):this.type  = set(isotonic,a)
}

trait HasBandwidth extends ConfigLike{
  final val bandwidth = new Param[Double](this,"bandwidth"," the bandwidth (standard deviation) of the Gaussian kernel.") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(1.0)

  def setBandwidth(a:Option[Double]):this.type = set(bandwidth,a)
}

trait HasPeacePeriod extends ConfigLike{
  final val peacePeriod = new Param[Int](this,"peacePeriod","the number of initial batches to ignore.") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(0)

  def setPeacePeriod(a:Option[Int]):this.type = set(peacePeriod,a)
}

trait HasWindowSize extends ConfigLike{
  final val windowSize = new Param[Int](this,"windowSize","the number of batches to compute significance tests over.") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(0)

  def setWindowSize(a:Option[Int]):this.type = set(windowSize,a)
}

trait HasWithStd extends ConfigLike{
  final val withStd = new Param[Boolean](this,"WithStd","Whether to scale the data to unit standard deviation.") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }

  def setWithStd(a:Option[Boolean]):this.type = set(withStd,a)
}

trait HasWithMean extends ConfigLike{
  final val withMean = new Param[Boolean](this,"WithMean","Whether to center the data with mean before scaling.") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }

  def setWithMean(a:Option[Boolean]):this.type = set(withMean,a)
}

trait HasWithLabels extends ConfigLike{
  final val withLabels = new Param[Boolean](this,"withLabels","Whether create vector with column names as labels.") {
    @transient override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(false)

  def setWithLabels(b:Option[Boolean]):this.type = set(withLabels,b)
}

trait HasTestMethod extends ConfigLike{
  final val testMethod = new Param[String](this,"testMethod","the statistical method used for significance testing.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("welch")

  def setTestMethod(a:Option[String]):this.type = set(testMethod,a)
}

trait HasPrecisionAt extends ConfigLike{
  final val precisionAt = new Param[Int](this,"precisionAt","the average precision of all the queries") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(2)

  def setPrecisionAt(a:Option[Int]):this.type = set(precisionAt,a)
}

trait HasNdcgAt extends ConfigLike{
  final val ndcgAt = new Param[Int](this,"ndcgAt","the average NDCG value of all the queries") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(2)

  def setNdcgAt(a:Option[Int]):this.type = set(ndcgAt,a)
}

trait HasLimit extends ConfigLike{
  final val limit = new Param[Double](this,"limit"," the upper limit.") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(0.8)

  def setLimit(a:Option[Double]):this.type = set(limit,a)
}

trait HasNumRows extends ConfigLike{
  final val numRows = new Param[Int](this,"numRows","the number of columns") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }

  def setNumRows(a:Option[Int]):this.type = set(numRows,a)
}

trait HasNumCols extends ConfigLike{
  final val numCols = new Param[Int](this,"numCols","the number of rows") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }

  def setNumCols(a:Option[Int]):this.type = set(numCols,a)
}

trait HasRowLabels extends ConfigLike{
  final val rowLabels = new Param[Seq[String]](this,"rowLabels","the labels of rows.") {
    @transient override val format: Format[Option[Seq[String]]] = FORMAT.SEQ.STRING
  }.default(Seq("count","max","min","mean","normL1","normL2","numNonzeros","variance"))

  def setRowLabels(a:Option[Seq[String]]):this.type = set(rowLabels,a)
}

trait HasColLabels extends ConfigLike{
  final val colLabels = new Param[Seq[String]](this,"ColLabels","the labels of columns.") {
    @transient override val format: Format[Option[Seq[String]]] = FORMAT.SEQ.STRING
  }.required()

  def setColLabels(a:Option[Seq[String]]):this.type = set(colLabels,a)
}

trait HasTransposed extends ConfigLike{
  final val transposed = new Param[Boolean](this,"transposed","whether or not is transposed") {
    override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(true)

  def setTransposed(a:Option[Boolean]):this.type = set(transposed,a)
}

trait HasNumBins extends ConfigLike{
  final val numBins = new Param[Int](this,"numBins","the number of bins") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(1)

  def setNumBins(a:Option[Int]):this.type = set(numBins,a)
}

trait HasBinary extends ConfigLike{
  final val binary = new Param[Boolean](this,"Binary","Binary toggle to control term frequency counts.") {
    override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(true)

  def setBinary(a:Option[Boolean]):this.type = set(binary,a)
}

trait HasMinDocFreq extends ConfigLike{
  final val minDocFreq = new Param[Int](this,"MinDocFreq","The minimum number of documents in which a term should appear.") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(0)

  def setMinDocFreq(a:Option[Int]):this.type = set(minDocFreq,a)
}

trait HasMaxSentenceLength extends ConfigLike{
  final val maxSentenceLength = new Param[Int](this,"maxSentenceLength","Sets the maximum length (in words) of each sentence in the input data.") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(300)

  def setMaxSentenceLength(a:Option[Int]):this.type = set(maxSentenceLength,a)
}

trait HasCaseSensitive extends ConfigLike{
  final val caseSensitive = new Param[Boolean](this,"caseSensitive","Whether to do a case sensitive comparison over the stop words.") {
    override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(false)

  def setCaseSensitive(a:Option[Boolean]):this.type = set(caseSensitive,a)
}

trait HasDropLast extends ConfigLike{
  final val dropLast = new Param[Boolean](this,"DropLast","Whether to drop the last category in the encoded vector.") {
    override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(true)

  def setDropLast(a:Option[Boolean]):this.type = set(dropLast,a)
}

trait HasMax extends ConfigLike{
  final val max = new Param[Double](this,"max","upper bound after transformation, shared by all features") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(1.0)

  def setMax(a:Option[Double]):this.type = set(max,a)
}

trait HasMin extends ConfigLike{
  final val min = new Param[Double](this,"min","upper bound after transformation, shared by all features") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(0.0)

  def setMin(a:Option[Double]):this.type = set(min,a)
}

trait HasForceIndexLabel extends ConfigLike{
  final val forceIndexLabel = new Param[Boolean](this,"forceIndexLabel","Force to index label whether it is numeric or string type.") {
    override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(false)

  def setForceIndexLabel(a:Option[Boolean]):this.type = set(forceIndexLabel,a)
}

trait HasFpr extends ConfigLike{
  final val fpr = new Param[Double](this,"fpr","The highest p-value for features to be kept.") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }

  def setFpr(a:Option[Double]):this.type = set(fpr,a)
}

trait HasFdr extends ConfigLike{
  final val fdr = new Param[Double](this,"fdr","The upper bound of the expected false discovery rate.") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }

  def setFdr(a:Option[Double]):this.type = set(fdr,a)
}

trait HasFwe extends ConfigLike{
  final val fwe = new Param[Double](this,"fwe","The upper bound of the expected family-wise error rate.") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }

  def setFwe(a:Option[Double]):this.type = set(fwe,a)
}

trait HasPercentile extends ConfigLike{
  final val percentile = new Param[Double](this,"percentile","Percentile of features that selector will select, ordered by statistics value descending.") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }

  def setPercentile(a:Option[Double]):this.type = set(percentile,a)
}

trait HasSelectorType extends ConfigLike{
  final val selectorType = new Param[String](this,"selectorType","The selector type of the ChisqSelector.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }

  def setSelectorType(a:Option[String]):this.type = set(selectorType,a)
}

trait HasFitIntercept extends ConfigLike{
  final val fitIntercept = new Param[Boolean](this,"FitIntercept","Param for whether to fit an intercept term.") {
    override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(true)

  def setFitIntercept(a:Option[Boolean]):this.type = set(fitIntercept,a)
}

trait HasStandardization extends ConfigLike{
  final val standardization = new Param[Boolean](this,"Standardization","Param for whether to standardize the training features before fitting the model.") {
    override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(true)

  def setStandardization(a:Option[Boolean]):this.type = set(standardization,a)
}

trait HasTol extends ConfigLike{
  final val tol = new Param[Double](this,"Tol","Param for the convergence tolerance for iterative algorithms (>= 0).") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(1.0E-6)

  def setTol(a:Option[Double]):this.type = set(tol,a)
}

trait HasCheckpointInterval extends ConfigLike{
  final val checkpointInterval = new Param[Int](this,"CheckpointInterval","Param for set checkpoint interval (>= 1) or disable checkpoint (-1).") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(10)

  def setCheckpointInterval(a:Option[Int]):this.type = set(checkpointInterval,a)
}

trait HasMaxBins extends ConfigLike{
  final val maxBins = new Param[Int](this,"MaxBins","Maximum number of bins used for discretizing continuous features and for choosing how to split on features at each node.") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(32)

  def setMaxBins(a:Option[Int]):this.type = set(maxBins,a)
}

trait HasMaxDepth extends ConfigLike{
  final val maxDepth = new Param[Int](this,"MaxDepth","Maximum depth of the tree (>= 0).") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(5)

  def setMaxDepth(a:Option[Int]):this.type = set(maxDepth,a)
}

trait HasMinInstancesPerNode extends ConfigLike{
  final val minInstancesPerNode = new Param[Int](this,"MinInstancesPerNode","Minimum number of instances each child must have after split.") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(1)

  def setMinInstancesPerNode(a:Option[Int]):this.type = set(minInstancesPerNode,a)
}

trait HasMinInfoGain extends ConfigLike{
  final val minInfoGain = new Param[Double](this,"MinInfoGain","Minimum information gain for a split to be considered at a tree node.") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(0.0)

  def setMinInfoGain(a:Option[Double]):this.type = set(minInfoGain,a)
}

trait HasImpurity extends ConfigLike{
  final val impurity = new Param[String](this,"Impurity","Criterion used for information gain calculation (case-insensitive).") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("gini")

  def setImpurity(a:Option[String]):this.type = set(impurity,a)
}

trait HasFeatureSubsetStrategy extends ConfigLike{
  final val featureSubsetStrategy = new Param[String](this,"FeatureSubsetStrategy","The number of features to consider for splits at each tree node.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("auto")

  def setFeatureSubsetStrategy(a:Option[String]):this.type = set(featureSubsetStrategy,a)
}

trait HasSubsamplingRate extends ConfigLike{
  final val subsamplingRate = new Param[Double](this,"subsamplingRate","Fraction of the training data used for learning each decision tree, in range (0, 1].") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(1.0)

  def setSubsamplingRate(a:Option[Double]):this.type = set(subsamplingRate,a)
}

trait HasLossType extends ConfigLike{
  final val lossType = new Param[String](this,"LossType","Loss function which GBT tries to minimize.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("squared")

  def setLossType(a:Option[String]):this.type = set(lossType,a)
}

trait HasModelType extends ConfigLike{
  final val modelType = new Param[String](this,"ModelType","The model type which is a string (case-sensitive).") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("multinomial")

  def setModelType(a:Option[String]):this.type = set(modelType,a)
}

trait HasSmothing extends ConfigLike{
  final val smothing = new Param[Double](this,"smothing","The smoothing parameter.") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(1.0)

  def setSmothing(a:Option[Double]):this.type = set(smothing,a)
}

trait HasAggregationDepth extends ConfigLike{
  final val aggregationDepth = new Param[Int](this,"AggregationDepth","the depth of aggregation") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(2)

  def setAggregationDepth(a:Option[Int]):this.type = set(aggregationDepth,a)
}

trait HasSolver extends ConfigLike{
  final val solver = new Param[String](this,"Solver","Param for the solver algorithm for optimization.") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("auto")

  def setSolver(a:Option[String]):this.type = set(solver,a)
}

trait HasCacheNodeIds extends ConfigLike{
  final val cacheNodeIds = new Param[Boolean](this,"CacheNodeIds","") {
    override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(false)

  def setCacheNodeIds(a:Option[Boolean]):this.type = set(cacheNodeIds,a)
}

trait HasMaxMemoryInMB extends ConfigLike{
  final val maxMemoryInMB = new Param[Int](this,"maxMemoryInMB","Max number of memory intput.") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(256)

  def setMaxMemoryInMB(a:Option[Int]):this.type = set(maxMemoryInMB,a)
}

trait HasMaxInter extends ConfigLike{
  final val maxInter = new Param[Int](this,"maxInter","Param for maximum number of iterations (>= 0).") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(20)

  def setMaxInter(a:Option[Int]):this.type = set(maxInter,a)
}

trait HasFeatureIndex extends ConfigLike{
  final val featureIndex = new Param[Int](this,"featureIndex","Param for the index of the feature if featuresCol is a vector column ") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(0)

  def setFeatureIndex(a:Option[Int]):this.type = set(featureIndex,a)
}

trait HasMinDivisibleClusterSize extends ConfigLike{
  final val minDivisibleClusterSize = new Param[Int](this,"minDivisibleClusterSize","Minnum size of divisibleCluster ") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(2)

  def setMinDivisibleClusterSize(a:Option[Int]):this.type = set(minDivisibleClusterSize,a)
}

trait HasInitMode extends ConfigLike{
  final val initMode = new Param[String](this,"initMode","") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }

  def setInitMode(a:Option[String]):this.type = set(initMode,a)
}

trait HasInitSteps extends ConfigLike{
  final val initSteps = new Param[Int](this,"initSteps","The steps of init.") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }

  def setInitSteps(a:Option[Int]):this.type = set(initSteps,a)
}

trait HasAlpha extends ConfigLike{
  final val alpha = new Param[Double](this,"Alpha","Param for the alpha parameter in the implicit preference formulation (nonnegative).") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(1.0)

  def setAlpha(a:Option[Double]):this.type = set(alpha,a)
}

trait HasLambda extends ConfigLike{
  final val lambda = new Param[Double](this,"Lambda","Param for als mlib lambda") {
    @transient override val format: Format[Option[Double]] = FORMAT.DOUBLE
  }.default(0.01)

  def setLambda(a:Option[Double]):this.type = set(lambda,a)
}

trait HasFinalStorageLevel extends ConfigLike{
  final val finalStorageLevel = new Param[String](this,"FinalStorageLevel","") {
    @transient override val format: Format[Option[String]] = FORMAT.STRING
  }.default("MEMORY_AND_DISK")

  def setFinalStorageLevel(a:Option[String]):this.type = set(finalStorageLevel,a)
}

trait HasNonnegative extends ConfigLike{
  final val nonnegative = new Param[Boolean](this,"Nonnegative","Param for whether to apply nonnegativity constraints.") {
    override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(false)

  def setNonnegative(a:Option[Boolean]):this.type = set(nonnegative,a)
}

trait HasNumBlocks extends ConfigLike{
  final val numBlocks = new Param[Int](this,"NumUserBlocks","Param for number of blocks.") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(10)

  def setNumBlocks(a:Option[Int]):this.type = set(numBlocks,a)
}

trait HasNumItemBlocks extends ConfigLike{
  final val numItemBlocks = new Param[Int](this,"NumItemBlocks","Param for number of item blocks (positive).") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(10)

  def setNumItemBlocks(a:Option[Int]):this.type = set(numItemBlocks,a)
}

trait HasRank extends ConfigLike{
  final val rank = new Param[Int](this,"Rank","Param for rank of the matrix factorization (positive).") {
    override val format: Format[Option[Int]] = FORMAT.INT
  }.default(10)

  def setRank(a:Option[Int]):this.type = set(rank,a)
}

trait HasImplicitPrefs extends ConfigLike{
  final val implicitPrefs = new Param[Boolean](this,"ImplicitPrefs","Param to decide whether to use implicit preference.") {
    override val format: Format[Option[Boolean]] = FORMAT.BOOLEAN
  }.default(false)

  def setImplicitPrefs(a:Option[Boolean]):this.type = set(implicitPrefs,a)
}

trait HasMaxLocalProjDBSize extends ConfigLike{
  final val maxLocalProjDBSize = new Param[Long](this,"maxLocalProjDBSize","the maximum number of items allowed in a projected database before local processing.") {
    override val format: Format[Option[Long]] = FORMAT.LONG
  }

  def setMaxLocalProjDBSize(a:Option[Long]):this.type = set(maxLocalProjDBSize,a)
}
