// spamFilter.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object spamFilter {


  def probaWordDir(sc:SparkContext)(filesDir:String) :(RDD[(String, Double)], Long) = {
    println("Call to probaWordDir with args" + filesDir)
    // a. This function reads all the text files within a directory
    // wholeTextFiles get back a pair RDD where the key is the name of the input file. 
    val filesSpam = sc.wholeTextFiles("/" + filesDir + "/spam/*.txt").collect.toList
    val filesHam = sc.wholeTextFiles("/" + filesDir + "/ham/*.txt").collect.toList
    
    val files = filesHam ::: filesSpam

    // b. The number of files is counted and stored in a variable nbFiles
    val nbFiles = files.size
    
    // d. Non informative words must be removed from the set of unique words. The list of non-informative words is ".", ":", ",", " ", "/", "\", "-", "'", "(", ")", "@"
    // Map uses a function that is aplied to each element of the list. Flat map flats the list, removing nested lists (only one list with all elements)
    var nolistOfWords = List(".", ":", ",", " ", "/", "/", "-", "'","(", ")", "@")
    var listOfWordsSpam = filesSpam.flatMap(x => x._2.split("\\s+")).filterNot(x => nolistOfWords.contains(x))
    var listOfWordsHam = filesHam.flatMap(x => x._2.split("\\s+")).filterNot(x => nolistOfWords.contains(x))

    val listOfWords = listOfWordsHam ::: listOfWordsSpam

    // c. Each text file must be splitted into a set of unique words (if a word occurs several times, it is saved only one time in the set).
    // ListVar.distint is used to remove duplicate instances. Unlike toSet.toList .distint preserves the previous order.
    //var listOfUniqueWordsSpam = listOfWordsSpam.distinct
    //var listOfUniqueWordsHam = listOfWordsSpam.distinct
    //var listOfUniqueWords = listOfUniqueWordsHam ::: listOfUniqueWordsSpam

    // e. read each file in fileStrings and for the word returns the number of files in which occurs in the directory. 
    // In a Map structure
    var mapOfWords = listOfWords.groupBy(x => x).map(x._1, (x._2.size).toFloat/nbFiles))

    
    // map structure: word => number of occurrences (files) of this word
    val probaWord :RDD[(String, Double)] =sc.parallelize(mapOfWords.toSeq)
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





