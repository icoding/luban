package grimtest.ml.feature

import grimoire.ml.feature.transform._
import grimoire.spark.source.dataframe.CSVSource
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-6-22.
  */
object ChiSqSelectorTest {
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val chisq = CSVSource("""{"InputPath":"data/chisq.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label double"}""").
    cast(DataFrameVectorAssemblerSpell().setInputCols(Seq("f1","f2","f3","f4")).setOutputCol("vec")).
    cast(DataFrameChiSqSelectorSpell().setNumTopFeatures(1).setFeaturesCol("vec").setLabelCol("label").setOutputCol("chisq"))

  val slicer = CSVSource("""{"InputPath":"data/chisq.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label double"}""").
    cast(DataFrameVectorAssemblerSpell().setInputCols(Seq("f1","f2","f3","f4")).setOutputCol("vec")).
    cast(DataFrameVectorSlicerSpell().setInputCol("vec").setOutputCol("slicer").setIndices(Seq(1)).setNames(Seq("f3")))

  val formu = CSVSource("""{"InputPath":"data/chisq.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label double"}""").
    cast(DataFrameRFormulaSpell().setFormula("label ~ f1 + f3").setFeaturesCol("vec").setLabelCol("label"))

  val trans = CSVSource("""{"InputPath":"data/chisq.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label double"}""").
    cast(DataFrameVectorAssemblerSpell().setInputCols(Seq("f1","f2","f3","f4")).setOutputCol("vec")).
    cast(DataFrameElementwiseProductSpell().setInputCol("vec").setOutputCol("trans").setScalingVec(Seq(0.0,1.0,2.0,3.0)))

}
