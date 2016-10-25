// spamFilter.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object spamFilter {


  def probaWordDir(sc:SparkContext)(filesDir:String) :(RDD[(String, Double)], Long) = {
    println("Call to probaWordDir with args" + filesDir)
    // This function reads all the text files within a directory
    // wholeTextFiles get back a pair RDD where the key is the name of the input file.
    val files  = sc.wholeTextFiles("/" + filesDir + "/*.txt").collect.toList.map(x => x._1)
    // The number of files is counted and stored in a variable nbFiles
    val nbFiles = files.size
    // Each text file must be splitted into a set of unique words (if a word occurs several times, it is saved only one time in the set).
    // toSet is used to remove duplicate words. listOfWords is the set of unique words
    var listOfWords = files.flatMap(x => (sc.textFile(x).flatMap(_.split("\\s+"))).collect.toSet.toList).toSet
    // Non informative words must be removed from the set of unique words. The list of non-informative words is ".", ":", ",", " ", "/", "\", "-", "'", "(", ")", "@"
    listOfWords = listOfWords - (".", ":", ",", " ", "/", "/", "-", "'","(", ")", "@")
    // read each file in fileStrings and for the word returns the number of files in which occurs in the directory
    def dfCalc(t:String, D: List[String]): Int = D.flatMap(x => sc.textFile(x).flatMap(_.split("\\s+")).filter(x => x==t).collect.toSet).count(x => x.size>0)
    // map structure: word => number of occurrences (files) of this word
    val probaWord :RDD[(String, Double)] =sc.parallelize(listOfWords.map(x => (x, (dfCalc(x, files) / nbFiles).toDouble)).toSeq)
    (probaWord, nbFiles)
  }

  def computeMutualInformationFactor(
    probaWC:RDD[(String, Double)],
    probaW:RDD[(String, Double)],
    probaC: Double,
    probaDefault: Double // default value when a probability is missing
  ):RDD[(String, Double)] = {
  // Code to complete..


  }


  def main(args: Array[String]) {
    if(args.length>0)
      println("Started Spam Filter with arg = " + args(0))
    else
      println("You should provide a directory path ")

    //val conf = new SparkConf().setAppName("Spam Filter Application")
    val conf = new SparkConf().setMaster("local").setAppName("Spam Filter Application")
    val sc = new SparkContext(conf)
    val (probaW, nbFiles)= probaWordDir(sc)(args(0))
    // Compute the couples (probaWordHam, nbFilesHam) for the directory “ham” and (probaWordSpam, nbFilesSpam) for the directory “spam”
    val (probaWordH,nbFilesH) = (probaWordDir(sc)(args(0).concat( "/" + "ham")))
    val (probaWordS,nbFilesS) = (probaWordDir(sc)(args(0).concat( "/" + "spam")))
    //Compute the probability P(occurs, class) for each word.
    val probaWC = (probaWordH,probaWordS,probaWordH.map(x => (x._1,1-x._2)),probaWordS.map(x => (x._1,1-x._2)))
    // RDD with the map structure: word => probability the word occurs (or not) in an email of a given class.
    val probaC  // the probability that an email belongs to the given class.
    computeMutualInformationFactor(probaWC,probaW,probaC,10) // the last is a default value

  }

} // end of spamFilter 





