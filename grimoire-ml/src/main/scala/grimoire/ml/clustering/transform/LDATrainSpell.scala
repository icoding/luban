package grimoire.ml.clustering.transform

import grimoire.configuration.param.{HasInputCol, HasMaxIter, HasOutputCol, Param, ParamValidators}
import grimoire.ml.configuration.param.{HasCheckpointInterval, HasK, HasSeed}
import grimoire.transform.Spell
import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.{Format, JsPath, JsValue}

/**
  * Created by caphael on 2017/3/22.
  */
class LDATrainSpell extends Spell[DataFrame,LDAModel]
  with HasK
  with HasMaxIter
  with HasSeed
  with HasInputCol
  with HasOutputCol
  with HasCheckpointInterval{

//  LDA Parameters Declaration
  final val docConcentration = new Param[Seq[Double]](this, "DocConcentration",
    "Concentration parameter (commonly named \"alpha\") for the prior placed on documents'" +
      " distributions over topics (\"theta\").", (alpha: Seq[Double]) => alpha.forall(_ >= 0.0)) {
    @transient override val format: Format[Option[Seq[Double]]] = (JsPath \ name).formatNullable[Seq[Double]]
  }
  def setDocConcentration(a:Option[Seq[Double]]):this.type = set(docConcentration,a)

  final val topicConcentration = new Param[Double](this, "TopicConcentration",
    "Concentration parameter (commonly named \"beta\" or \"eta\") for the prior placed on topic'" +
      " distributions over terms.", ParamValidators.ge(0)) {
    @transient override val format: Format[Option[Double]] = (JsPath \ name).formatNullable[Double]
  }
  def setTopicConcentration(a:Option[Double]) = set(topicConcentration,a)

  final val supportedOptimizers: Array[String] = Array("online", "em")
  final val optimizer = new Param[String](this, "Optimizer", "Optimizer or inference" +
    " algorithm used to estimate the LDA model. Supported: " + supportedOptimizers.mkString(", "),
    (o: String) => ParamValidators.inArray(supportedOptimizers).apply(o.toLowerCase)) {
    @transient override val format: Format[Option[String]] = (JsPath \ name).formatNullable[String]
  }.default("online")
  def setOptimizer(a:Option[String]) = set(optimizer,a)

  final val learningOffset = new Param[Double](this, "LearningOffset", "(For online optimizer)" +
    " A (positive) learning parameter that downweights early iterations. Larger values make early" +
    " iterations count less.",
    ParamValidators.gt(0)) {
    @transient override val format: Format[Option[Double]] = (JsPath \ name).formatNullable[Double]
  }
  def setLearningOffset(a:Option[Double]) = set(learningOffset,a)

  final val learningDecay = new Param[Double](this, "LearningDecay", "(For online optimizer)" +
    " Learning rate, set as an exponential decay rate. This should be between (0.5, 1.0] to" +
    " guarantee asymptotic convergence.", ParamValidators.gt(0)) {
    @transient override val format: Format[Option[Double]] = (JsPath \ name).formatNullable[Double]
  }
  def setLearningDecay(a:Option[Double]) = set(learningDecay,a)

  final val subsamplingRate = new Param[Double](this, "SubsamplingRate", "(For online optimizer)" +
    " Fraction of the corpus to be sampled and used in each iteration of mini-batch" +
    " gradient descent, in range (0, 1].",
    ParamValidators.inRange(0.0, 1.0, lowerInclusive = false, upperInclusive = true)) {
    @transient override val format: Format[Option[Double]] = (JsPath \ name).formatNullable[Double]
  }
  def setSubsamplingRate(a:Option[Double]) = set(subsamplingRate,a)

  final val optimizeDocConcentration = new Param[Boolean](this, "OptimizeDocConcentration",
    "(For online optimizer only, currently) Indicates whether the docConcentration" +
      " (Dirichlet parameter for document-topic distribution) will be optimized during training.") {
    @transient override val format: Format[Option[Boolean]] = (JsPath \ name).formatNullable[Boolean]
  }
  def setOptimizeDocConcentration(a:Option[Boolean]) = set(optimizeDocConcentration,a)

  final val keepLastCheckpoint = new Param[Boolean](this, "KeepLastCheckpoint",
    "(For EM optimizer) If using checkpointing, this indicates whether to keep the last" +
      " checkpoint. If false, then the checkpoint will be deleted. Deleting the checkpoint can" +
      " cause failures if a data partition is lost, so set this bit with care.") {
    @transient override val format: Format[Option[Boolean]] = (JsPath \ name).formatNullable[Boolean]
  }
  def setKeepLastCheckpoint(a:Option[Boolean]) = set(keepLastCheckpoint,a)

  override def transformImpl(dat: DataFrame): LDAModel = {
    val lda = new LDA()
      .setK($(k))
      .setMaxIter($(maxIter))
      .setFeaturesCol($(inputCol))
      .setTopicDistributionCol($(outputCol))
      .setOptimizer($(optimizer))
      .setCheckpointInterval($(checkpointInterval))


    if (docConcentration.isSet) lda.setDocConcentration($(docConcentration).toArray)
    if (topicConcentration.isSet) lda.setTopicConcentration($(topicConcentration))
    if (learningOffset.isSet) lda.setLearningOffset($(learningOffset))
    if (subsamplingRate.isSet) lda.setSubsamplingRate($(subsamplingRate))
    if (optimizeDocConcentration.isSet) lda.setOptimizeDocConcentration($(optimizeDocConcentration))
    if (keepLastCheckpoint.isSet) lda.setKeepLastCheckpoint($(keepLastCheckpoint))
    if (seed.isSet) lda.setSeed($(seed))

    lda.fit(dat)
  }
}

object LDATrainSpell{
  def apply(json: JsValue): LDATrainSpell =
    new LDATrainSpell().parseJson(json)
}