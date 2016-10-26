// spamFilter.scala
import org.apache.spark.rdd.RDD._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object spamFilter {


  def probaWordDir(sc:SparkContext)(filesDir:String) :(RDD[(String, Double)], Long) = {
    println("Call to probaWordDir with args" + filesDir)
    // This function reads all the text files within a directory
    // wholeTextFiles get back a pair RDD where the key is the name of the input file.
    val filesText  = sc.wholeTextFiles(filesDir + "/*/*.txt").collect.toList
    // The number of files is counted and stored in a variable nbFiles
    val nbFiles = filesText.size
    // Each text file must be splitted into a set of unique words (if a word occurs several times, it is saved only one time in the set).
    // toSet is used to remove duplicate words. listOfWords is the set of unique words
    val listOfWords : Set[String] = filesText.flatMap(x =>x._2.split("\\s+")).toSet - (".", ":", ",", " ", "/", "/", "-", "'","(", ")", "@")
    // Non informative words must be removed from the set of unique words. The list of non-informative words is ".", ":", ",", " ", "/", "\", "-", "'", "(", ")", "@"
    // read each file in fileStrings and for the word returns the number of files in which occurs in the directory
    def dfCalc(t:String, D: List[(String,String)]): Int = D.map(x => x._2.split("\\s+").filter(x => x==t)).count(x => x.nonEmpty)
    // map structure: word => number of occurrences (files) of this word
    val probaWord  =sc.parallelize(listOfWords.map(x => (x, dfCalc(x, filesText).toDouble / nbFiles)).toSeq)
    (probaWord, nbFiles)
  }

  def computeMutualInformationFactor(
    probaWC:RDD[(String, Double)],
    probaW:RDD[(String, Double)],
    probaC: Double,
    probaDefault: Double // default value when a probability is missing
  ):RDD[(String, Double)] = {
    val probWJoin : RDD[(String,(Option[Double],Option[Double]))]  = probaW.fullOuterJoin(probaWC)
    probWJoin.map(x=>(x._1,x._2._1.get*math.log(x._2._1.get/(x._2._2.get*probaC))))
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
    val (probaWordH,nbFilesH) = probaWordDir(sc)(args(0).concat( "/" + "ham"))
    val (probaWordS,nbFilesS) = probaWordDir(sc)(args(0).concat( "/" + "spam"))
    //Compute the probability P(occurs, class) for each word.
    // RDD with the map structure: word => probability the word occurs (or not) in an email of a given class.
    val probaWC = (probaWordH,probaWordS,probaWordH.map(x => (x._1,1-x._2)),probaWordS.map(x => (x._1,1-x._2)))
    val probaH = nbFilesH/nbFiles // the probability that an email belongs to the given class.
    val probaS = nbFilesS/nbFiles
    // Compute mutual information for each class and occurs
    var mutualInfH = computeMutualInformationFactor(probaWC._1,probaW,probaH,0.2/nbFiles) // the last is a default value
    var mutualInfS = computeMutualInformationFactor(probaWC._2,probaW,probaS,0.2/nbFiles)
    var mutualInfNh = computeMutualInformationFactor(probaWC._3,probaW,probaH,0.2/nbFiles)
    var mutualInfNs = computeMutualInformationFactor(probaWC._4,probaW,probaS,0.2/nbFiles)

    //compute the mutual information of each word as a RDD with the map structure: word => MI(word).
    

    /*var listOfWords = extractListOfWords(sc)(args(0))
    var mutualInfoW : RDD[(String,Double)]= listOfWords.map(x=>)*/

    //These words must be also stored on HDFS in the file “/tmp/topWords.txt”.

  }

} // end of spamFilter 





