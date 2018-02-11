package grimtest.ml.clustering

/**
  * Created by jax on 17-6-28.
  */
object KMeansTest {
  import grimoire.Implicits._
  import grimoire.spark.globalSpark
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import grimoire.ml.clustering.transform.DataFrameKMeansSpell
  import grimoire.spark.source.dataframe.LibSvmSource


  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

  val kmeans =
    LibSvmSource("""{"InputPath":"data/mllib/sample_kmeans_data.txt"}""").
      cast(DataFrameKMeansSpell().setK(2).setSeed(1L))

  val model = kmeans.conjure


  //prediction test
  val dataset = globalSpark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")
  val WSSSE = model.computeCost(dataset)
  println(s"Within Set Sum of Squared Errors = $WSSSE")

  // Shows the result.
  println("Cluster Centers: ")
  model.clusterCenters.foreach(println)
}
