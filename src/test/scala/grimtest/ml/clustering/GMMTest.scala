package grimtest.ml.clustering

/**
  * Created by jax on 17-6-28.
  */
object GMMTest {
  import grimoire.Implicits._
  import grimoire.spark.globalSpark
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import grimoire.ml.clustering.transform.DataFrameGMMSpell
  import grimoire.spark.source.dataframe.LibSvmSource


  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val gmm =
    LibSvmSource("""{"InputPath":"data/mllib/sample_kmeans_data.txt"}""").
      cast(DataFrameGMMSpell().setK(2))

  val model = gmm.conjure


  //prediction test
  for (i <- 0 until model.getK) {
    println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
      s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n")
  }
}
