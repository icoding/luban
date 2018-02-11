
name := "grimoire"

lazy val commonSettings = Seq(
  organization := "magus",
  version := "0.1.0",
  scalaVersion := "2.11.8",
  libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0",
  libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.0",
  libraryDependencies += "org.apache.spark" % "spark-yarn_2.11" % "2.1.0",
  libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.1.0",
  libraryDependencies += "com.typesafe" % "config" % "1.3.1" ,
  libraryDependencies += "com.github.scopt" % "scopt_2.10" % "3.3.0" ,
  libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.40",
  libraryDependencies += "org.apache.zeppelin" % "zeppelin-display_2.11" % "0.7.2" % "provided",
  libraryDependencies += "com.typesafe.play" % "play-json_2.11" % "2.5.10",
  libraryDependencies += "org.ddahl" %% "rscala" % "2.2.2",

  dependencyOverrides ++= Set(
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
    "io.netty" % "netty" % "3.10.3.Final"
  )
)

//unmanagedJars in Compile += file("/home/sjc505/IdeaProjects/scala/rscala/inst/java/scala-2.11/rscala_2.11-2.2.2.jar")

lazy val grimoire = (project in file("."))
  .settings(
    commonSettings
      :_*
  )
  .aggregate(util,core,operation,common,spark,ml,nlp,zeppelin)
  .dependsOn(util,core,operation,common,spark,ml,nlp,zeppelin)

lazy val util = (project in file("grimoire-util"))
  .settings(
    commonSettings :+
      (name := "grimoire-util") :+
      (libraryDependencies += "org.clapper" %% "classutil" % "1.1.2")
      :_*
  )

lazy val core = (project in file("grimoire-core"))
  .settings(
    commonSettings :+
      (name := "grimoire-core")
      :_*
  ).dependsOn(util)

lazy val common = (project in file("grimoire-common"))
  .settings(
    commonSettings :+
      (name := "grimoire-common")
      :_*
  ).dependsOn(core)

lazy val spark = (project in file("grimoire-spark"))
  .settings(
    commonSettings :+
      (name := "grimoire-spark")
      :_*
  ).dependsOn(core,common)

lazy val nlp = (project in file("grimoire-nlp"))
  .settings(
    commonSettings :+
      (name := "grimoire-nlp") :+
      (libraryDependencies += "com.huaban" % "jieba-analysis" % "1.0.0") :+
      (libraryDependencies += "com.hankcs" % "hanlp" % "portable-1.3.2") :+
      (libraryDependencies += "org.scalanlp" %% "breeze" % "0.10" ) :+
      (libraryDependencies += "org.scalanlp" %% "breeze-natives" % "0.10")
      :_*
  ).dependsOn(core,spark)

lazy val ml = (project in file("grimoire-ml"))
  .settings(
    commonSettings :+
      (name := "grimoire-ml")
      :_*
  ).dependsOn(core,spark,nlp)

lazy val operation = (project in file("grimoire-operation"))
  .settings(
    commonSettings :+
      (name := "grimoire-operation")
      :_*
  ).dependsOn(core)

lazy val job = (project in file("grimoire-job"))
  .settings(
    commonSettings :+
      (name := "grimoire-job") :+
      (unmanagedBase := baseDirectory.value / "lib")
      :_*
  ).dependsOn(ml,operation)

lazy val streaming = (project in file("grimoire-streaming"))
  .settings(
    commonSettings :+
      (name := "grimoire-streaming")
      :_*
  ).dependsOn(spark,operation)

lazy val service = (project in file("grimoire-service"))
  .settings(
    commonSettings :+
      (name := "grimoire-service")
      :_*
  ).dependsOn(spark,operation)

lazy val zeppelin = (project in file("grimoire-zeppelin"))
  .settings(
    commonSettings :+
      (name := "grimoire-zeppelin") :+
      (libraryDependencies += "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.6")
      :_*
  ).dependsOn(core,spark,ml)

