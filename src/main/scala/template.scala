// spamFilter.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object spamFilter {

  def probaWordDir(sc:SparkContext)(filesDir:String)
  :(RDD[(String, Double)], Long) = {

    // Code to complete...
    // This function reads all the text files within a directory named filesDir
    Val files = sc.wholeTextFiles(""/"" + filesDir + "/*.txt").collect.toList
    //sc.wholeTextFiles("/tmp/tp4/*.txt").collect.toList.map(x => x._1)

    // The number of files is counted and stored in a variable nbFiles
    Val nbFiles  = files.size

    // Each text file must be splitted into a set of unique words (if a word occurs several times, it is saved only one time in the set).
    // var listOfWords = filesStrings.flatMap(x => (sc.textFile(x).flatMap(_.split("\\s+"))).collect.toSet.toList)


    // Non informative words must be removed from the set of unique words. The list of non-informative words is ".", ":", ",", " ", "/", "\", "-", "'", "(", ")", "@"
    
    
    (probaWord, nbFiles)
  }


  def computeMutualInformationFactor(
    probaWC:RDD[(String, Double)],
    probaW:RDD[(String, Double)],
    probaC: Double,
    probaDefault: Double // default value when a probability is missing
  ):RDD[(String, Double)] = {

  // Code to complete...

  }

  def main(args: Array[String]) {

  // Code to complete...
  val conf = new SparkConf().setAppName("Spam Filter Application")
  val sc = new SparkContext(conf)
  this.probaWordDir(args(0))

  }

} // end of spamFilter 





