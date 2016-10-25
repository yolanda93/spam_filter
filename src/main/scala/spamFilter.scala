// spamFilter.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object spamFilter {


  def probaWordDir(sc:SparkContext)(filesDir:String) :(RDD[(String, Double)], Long) = {
    println("Call to probaWordDir with args" + filesDir)
    // wholeTextFiles get back a pair RDD where the key is the name of the input file.
    val rdd = sc.wholeTextFiles(filesDir)
    // The number of files is counted and stored in a variable nbFiles
    val nbFiles = rdd.count()
    // Non informative words must be removed from the set of unique words.
    val stopWords = Set(".", ":", ",", " ", "/", "\\", "-", "'", "(", ")", "@")
    // Each text file must be splitted into a set of unique words (if a word occurs several times, it is saved only one time in the set).
    val wordBagRdd: RDD[(String,Set[String])] = rdd.map(textTuple => (textTuple._1,textTuple._2.split(" ").toSet.diff(stopWords)))
    // Get the Number of occurrences amongst all files
    val wordCountRdd = wordBagRdd.flatMap(x => x._2.flatMap(x=> (x,1)))
    val probaWord :RDD[(String, Double)] = null
    //val probaWord :RDD[(String, Double)] =sc.parallelize(listOfWords.map(x => (x, (dfCalc(x, files) / nbFiles).toDouble)).toSeq)
    (probaWord, nbFiles)
  }

  /*def computeMutualInformationFactor(
    probaWC:RDD[(String, Double)],
    probaW:RDD[(String, Double)],
    probaC: Double,
    probaDefault: Double // default value when a probability is missing
  ):RDD[(String, Double)] = {
  // Code to complete..
  }*/

  // RDD with the map structure: word => probability the word occurs (or not) in an email of a given class.
  //def probaWordClass(word:String,clase:String,filesDir:String): (RDD[(String, Double)]) = {
    // compute the probaOccurs for clase emails

  //}

  def main(args: Array[String]) {
    if(args.length>0)
      println("Started Spam Filter with arg = " + args(0))
    else
      println("You should provide a directory path ")

  // Code to complete...
    val conf = new SparkConf().setMaster("local").setAppName("Spam Filter Application")
    val fileDir = "hdfs://tmp/ling-spam"
    val sc = new SparkContext(conf)
    val (probaW, nbFiles)= probaWordDir(sc)(fileDir)
    print(probaW, nbFiles)
    //val probaWC // RDD with the map structure: word => probability the word occurs (or not) in an email of a given class.
    //val probaC  // the probability that an email belongs to the given class.
    //computeMutualInformationFactor(probaWC,probaW,probaC,10) // the last is a default value

  }

} // end of spamFilter 





