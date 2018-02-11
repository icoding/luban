package grimtest.test

import org.ddahl.rscala._
import breeze.stats.distributions._
import breeze.linalg._

/**
  * Created by sjc505 on 17-7-28.
  */

object ScalaToRTest {

    // first simulate some data consistent with a Poisson regression model
    val x = Uniform(50,60).sample(1000)
    val eta = x map { x => (x * 0.1) - 3 }
    val mu = eta map { math.exp(_) }
    val y = mu map { Poisson(_).draw }

    val R = RClient() // initialise an R interpreter
   // R.d =R.eval("df1 <- data.frame(name=c(1,2,3),age=c(20,29,30),sex=c(10,20,30))")
    R.x=x.toArray // send x to R
    R.y=y.toArray // send y to R
    R.eval("mod <- glm(y~x,family=poisson())")// fit the model in R

    // pull the fitted coefficents back into scala
    val beta = DenseVector[Double](R.evalD1("mod$coefficients"))

    // print the fitted coefficents
    println(beta)

    val colnames = R.evalS1("colnames(iris)")
    val coltypes = R.evalS1("class(iris[,a])")
    R.eval("")

//    val coltype = df.select("f1").schema.map(f=>f.dataType.typeName)
//    def getTypes(coltypes:Seq[String]):Any = coltypes match {
//        case List(double) => R.set("f1",df.select("f1").collect.map(_.getDouble(0)))
//        case List(int) => R.set("f1",df.select("f1").collect.map(_.getInt(0)))
//        case List(string) => R.set("f1",df.select("f1").collect.map(_.getString(0)))
//    }
//    getTypes(coltype)

}


