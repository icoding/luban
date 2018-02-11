package grimoire.util

/**
  * Created by caphael on 2017/2/18.
  */
import java.io.PrintWriter

import org.clapper.classutil.ClassFinder

import scala.util.parsing.combinator.RegexParsers
import scala.util.parsing.input.CharSequenceReader

object ClassUtils {
  val grimClassPat = """^grimoire\.[^$]*(transform|source|target)\.[^$]*$""".r.pattern

  def getGrimoireClasses() ={
    val finder = ClassFinder()
    val classes = finder.getClasses()
    classes.filter(ci=> ci.isConcrete && grimClassPat.matcher(ci.name).find && !ci.name.endsWith("package"))
  }

  def genStuffConfFile(cfile:String = "conf/stuffs.conf") = {
    val header = "#Alias\tSimpleName\tClassName\tComment"

    val classesArr = getGrimoireClasses().toArray
    val simpleNames = classesArr.map(_.name.split("\\.").last)
    val aliases = simpleNames.map{
      case n=>
        classSimpleNameParser.parse(n).mkString("-").toLowerCase
    }

    val confEntries = padding(aliases).zip(padding(simpleNames)).zip(padding(classesArr.map(_.name))).map{
      case ((a,b),c) => (a,b,c)
    }

    val toWrite = Array(header) ++ confEntries.map{
      case (a,b,c)=>
        s"${a}\t${b}\t${c}"
    }

    val out = new PrintWriter(cfile)
    toWrite.foreach(out.println(_))
    out.close()

  }

  def padding(s:Array[String]):Array[String] = {
    def mkWS(n:Int):String = {
      (0 to n).map(x=>" ").mkString
    }
    val maxLen = s.map(_.length).max
    s.map(
      x =>
        x + mkWS(maxLen - x.length)
    )
  }

  object classSimpleNameParser extends RegexParsers {

    val _pSection = "[a-z]+[A-Z]".r | "[A-Z]+".r
    val _SimpleName = rep(_pSection) ^^ {
      case l=> l
    }

    def parse(text:String)= {
      _SimpleName(new CharSequenceReader(text.reverse)) match {
        case Success(r,_) => r.map(_.reverse).reverse
      }
    }
  }

}
