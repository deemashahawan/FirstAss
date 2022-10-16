import org.apache.commons.io.FilenameUtils
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.SparkContext
import scala.io.StdIn._
object Main {
  def main(args: Array[String]): Unit = {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)
    val sc = new SparkContext("local[*]", "test")
    //read whole files
   val myfilerdd = sc.wholeTextFiles("C:/Users/Deema/BD/FirstScalaAss/files")
    //split new rdd to new format (word 1 doc_name)
    val rdd1=myfilerdd.flatMap(f => f._2.split("\\W+")
      .map(el => (el,(1 ,FilenameUtils.getName(f._1)))))
    //reduce new rdd by key(word)
    val rdd2 =rdd1.reduceByKey((x, y) => (x._1 + y._1, (x._2 + " "+y._2)))
       .map(e => (e._1, e._2._1 , e._2._2)).sortBy(x=>x._1).map(x=>s"${x._1},${x._2},${x._3}")
    //rdd2.coalesce(1).saveAsTextFile("C:/Users/Deema/BD/FirstScalaAss/wholeInvertedIndex.txt")
    println("enter a name: ")
    val input =readLine()
    val output=rdd2.filter(x=>x.contains(input)).collect().mkString.split(",")(2)
    println(output)
 }

}
