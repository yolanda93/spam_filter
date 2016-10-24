// spamFilter.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object spamFilter {


  def probaWordDir(sc:SparkContext)(filesDir:String):(RDD[(String, Double)], Long) = {
    println("Call to probaWordDir with args" + filesDir)

    // This function reads all the text files within a directory
    // wholeTextFiles get back a pair RDD where the key is the name of the input file.
    val files = sc.wholeTextFiles("/" + filesDir + "/*.txt").collect.toList.map(x => x._1)
    // The number of files is counted and stored in a variable nbFiles
    val nbFiles = files.size
    // Each text file must be splitted into a set of unique words (if a word occurs several times, it is saved only one time in the set).
    // toSet is used to remove duplicate words. listOfWords is the set of unique words
    var listOfWords = files.flatMap(x => (sc.textFile(x).flatMap(_.split("\\s+"))).collect.toSet.toList)
    // Non informative words must be removed from the set of unique words. The list of non-informative words is ".", ":", ",", " ", "/", "\", "-", "'", "(", ")", "@"
    listOfWords = listOfWords - (".", ":", ",", " ", "/", ""/"", "-", "'","(", ")", "@")
    // read each file in fileStrings and for each word in listOfWords returns the number of ocurrencies in the directory
    def dfCalc(t:String, D: List[String]) = D.flatMap(x => (sc.textFile(x).flatMap(_.split("\\s+")).filter(x => x==t).collect).toSet).groupBy(x => x).mapValues(x => x.size)
    // map structure: word => number of occurrences (files) of this word.
    var wordDirOccurency = listOfWords.map(x => dfCalc(x, files))
    // map structure: word => probability of occurrences of the word.
    val probaWord = wordDirOccurency.map(x => wordDirOccurency.foreach{_.map(x => x._2/nbFiles)})
    (probaWord, nbFiles)
  }


/*  def computeMutualInformationFactor(
    probaWC:RDD[(String, Double)],
    probaW:RDD[(String, Double)],
    probaC: Double,
    probaDefault: Double // default value when a probability is missing
  ):RDD[(String, Double)] = {

  // Code to complete...

  }*/

  def main(args: Array[String]) {
    if(args.length>0)
      println("Started Spam Filter with arg = " + args(0))
    else
      println("You should provide a directory path ")

  // Code to complete...
    //val conf = new SparkConf().setAppName("Spam Filter Application")
    val conf = new SparkConf().setMaster("local").setAppName("Spam Filter Application")
    val sc = new SparkContext(conf)
    probaWordDir(sc)(args(0))

  }

} // end of spamFilter 





