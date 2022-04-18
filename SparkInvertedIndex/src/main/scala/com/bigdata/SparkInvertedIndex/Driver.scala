package src.main.scala.com.bigdata.SparkInvertedIndex

/*#############################################################################################
# Description: Spark - Inverted Index
#
# Input:
#   1. /user/chchang/week4/file1
#	2. /user/chchang/week4/file2
#   3. /user/chchang/week4/file3
#
# To Run this code use the command:
#  input
#							 /user/chchang/week4/file1 \
#							 /user/chchang/week4/file2 \
#              /user/chchang/week4/file3
#   output
#							 /user/chchang/week4_out/out.txt
#  spark-submit --master yarn --class src.main.scala.com.bigdata.SparkInvertedIndex.Driver  \
#              ./SparkInvertedIndex.jar  /user/chchang/week4 /user/chchang/week4_out/out
############################################################################################## */

// Scala Imports
import org.apache.spark.{SparkConf, SparkContext}


object Driver {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark - inverted Index")
    val sc = new SparkContext(conf)



    // Read input file
   sc.wholeTextFiles(args(0)).flatMap {
      case (path, text) =>
        text.split("""\W+""") map {
          //Create a tuple of (word, filePath)
          word => (word, path)
        }
    }.map {
      // Create a tuple with count 1 ((word, fileName), 1)
      case (w, p) => ((w, p.split("/")(6)), 1)
      //case (w, p) => ((w, p), 1)
    }.reduceByKey {
      // Group all (word, fileName) pairs and sum the counts
      case (n1, n2) => n1 + n2
    }.map {
      // Transform tuple into (word, (fileName, count))
      case ((w, p), n) => (w, (p, n))
    }.groupBy {
      // Group by words
      case (w, (p, n)) => w
    }.map {
      // Output sequence of (fileName, count) into a comma seperated string
      case (w, seq) =>
        val seq2 = seq map {
          case (_, (p, n)) => (p, n)
        }
        (w, seq2.mkString(", "))
    }.saveAsTextFile(args(1))

  }

}
