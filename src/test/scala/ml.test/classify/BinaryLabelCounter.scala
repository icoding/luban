package ml.test.classify


class BinaryLabelCounter(
                          var numPositives: Long = 0L,
                          var numNegatives: Long = 0L) extends Serializable {

  /** Processes a label. */
  def +=(label: Double): BinaryLabelCounter = {
    // Though we assume 1.0 for positive and 0.0 for negative, the following check will handle
    // -1.0 for negative as well.
    if (label > 0.5) numPositives += 1L else numNegatives += 1L
    this
  }

  /** Merges another counter. */
  def +=(other: BinaryLabelCounter): BinaryLabelCounter = {
    numPositives += other.numPositives
    numNegatives += other.numNegatives
    this
  }

  override def clone: BinaryLabelCounter = {
    new BinaryLabelCounter(numPositives, numNegatives)
  }

  override def toString: String = s"{numPos: $numPositives, numNeg: $numNegatives}"
}
