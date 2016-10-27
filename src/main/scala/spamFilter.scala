import org.apache.spark.rdd.RDD._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object spamFilter {


    def probaWordDir(sc:SparkContext)(filesDir:String) :(RDD[(String, Double)], Long) = {
      println("Call to probaWordDir with args" + filesDir)
      // This function reads all the text files within a directory
      // wholeTextFiles get back a pair RDD where the key is the name of the input file.
      val filesText  = sc.wholeTextFiles(filesDir).collect.toList
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
   val probWJoin : RDD[(String,(Option[Double],Option[Double]))]  = probaWC.fullOuterJoin(probaW)
   val valueClassAndOcu: RDD[(String, (Double, Double))] = probWJoin.map(x => (x._1, (x._2._1.getOrElse(probaDefault), x._2._2.getOrElse(probaDefault))))
   valueClassAndOcu.map(x => (x._1, x._2._1*math.log(x._2._1/(x._2._2*probaC))))
  }


  def main(args: Array[String]) {
    if(args.length>0)
      println("Started Spam Filter with arg = " + args(0))
    else
      println("You should provide the main directory path ")

    //val conf = new SparkConf().setAppName("Spam Filter Application")
    val conf = new SparkConf().setMaster("local").setAppName("Spam Filter Application")
    val sc = new SparkContext(conf)
    val (probaW, nbFiles)= probaWordDir(sc)(args(0) + "/*/*.txt")
    // Compute the couples (probaWordHam, nbFilesHam) for the directory “ham” and (probaWordSpam, nbFilesSpam) for the directory “spam”
    val (probaWordH,nbFilesH) = probaWordDir(sc)(args(0).concat("/ham/*.txt"))
    val (probaWordS,nbFilesS) = probaWordDir(sc)(args(0).concat("/spam/*.txt"))
    //Compute the probability P(occurs, class) for each word.
    // RDD with the map structure: word => probability the word occurs (or not) in an email of a given class.
    val probaWC = (probaWordH,probaWordS,probaWordH.map(x => (x._1,1-x._2)),probaWordS.map(x => (x._1,1-x._2)))
    val probaH = nbFilesH/nbFiles // the probability that an email belongs to the given class.
    val probaS = nbFilesS/nbFiles
    // Compute mutual information for each class and occurs
    var MITrueHam = computeMutualInformationFactor(probaWC._1,probaW,probaH,0.2/nbFiles) // the last is a default value
    var MITrueSpam = computeMutualInformationFactor(probaWC._2,probaW,probaS,0.2/nbFiles)
    var MIFalseHam = computeMutualInformationFactor(probaWC._3,probaW,probaH,0.2/nbFiles)
    var MIFalseSpam = computeMutualInformationFactor(probaWC._4,probaW,probaS,0.2/nbFiles)
    //compute the mutual information of each word as a RDD with the map structure: word => MI(word)
    var MI :RDD[(String, Double)] = MITrueHam.union(MITrueSpam).union(MIFalseHam).union(MIFalseSpam).reduceByKey( (x, y) => x + y)
    // print on screen the 10 top words (maximizing the mutual information value)
    //These words must be also stored on HDFS in the file “/tmp/topWords.txt”.
    var path : String = "/tmp/topWords.txt"
    sc.parallelize(MI.sortBy(_._2).takeOrdered(10)).keys.saveAsTextFile(path)
  }

} // end of spamFilter 

/*
Testing commands for spark-shell
      var  filesDir = "/home/yolanda/workspace/spam_filter/ling-spam" + "/*/*.txt"
      val filesText  = sc.wholeTextFiles(filesDir).collect.toList
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

      var  filesDir = "/home/yolanda/workspace/spam_filter/ling-spam" + "/ham/*.txt"
      val filesText  = sc.wholeTextFiles(filesDir).collect.toList
      // The number of files is counted and stored in a variable nbFiles
      val nbFiles = filesText.size
      // Each text file must be splitted into a set of unique words (if a word occurs several times, it is saved only one time in the set).
      // toSet is used to remove duplicate words. listOfWords is the set of unique words
      val listOfWords : Set[String] = filesText.flatMap(x =>x._2.split("\\s+")).toSet - (".", ":", ",", " ", "/", "/", "-", "'","(", ")", "@")
      // Non informative words must be removed from the set of unique words. The list of non-informative words is ".", ":", ",", " ", "/", "\", "-", "'", "(", ")", "@"
      // read each file in fileStrings and for the word returns the number of files in which occurs in the directory
      def dfCalc(t:String, D: List[(String,String)]): Int = D.map(x => x._2.split("\\s+").filter(x => x==t)).count(x => x.nonEmpty)
      // map structure: word => number of occurrences (files) of this word
      val probaWordH  =sc.parallelize(listOfWords.map(x => (x, dfCalc(x, filesText).toDouble / nbFiles)).toSeq)


      var  filesDir = "/home/yolanda/workspace/spam_filter/ling-spam" + "/spam/*.txt"
      val filesText  = sc.wholeTextFiles(filesDir).collect.toList
      // The number of files is counted and stored in a variable nbFiles
      val nbFiles = filesText.size
      // Each text file must be splitted into a set of unique words (if a word occurs several times, it is saved only one time in the set).
      // toSet is used to remove duplicate words. listOfWords is the set of unique words
      val listOfWords : Set[String] = filesText.flatMap(x =>x._2.split("\\s+")).toSet - (".", ":", ",", " ", "/", "/", "-", "'","(", ")", "@")
      // Non informative words must be removed from the set of unique words. The list of non-informative words is ".", ":", ",", " ", "/", "\", "-", "'", "(", ")", "@"
      // read each file in fileStrings and for the word returns the number of files in which occurs in the directory
      def dfCalc(t:String, D: List[(String,String)]): Int = D.map(x => x._2.split("\\s+").filter(x => x==t)).count(x => x.nonEmpty)
      // map structure: word => number of occurrences (files) of this word
      val probaWordS  =sc.parallelize(listOfWords.map(x => (x, dfCalc(x, filesText).toDouble / nbFiles)).toSeq)

      val probaWC = (probaWordH,probaWordS,probaWordH.map(x => (x._1,1-x._2)),probaWordS.map(x => (x._1,1-x._2)))
      val probaH = nbFilesH/nbFiles // the probability that an email belongs to the given class.
      val probaS = nbFilesS/nbFiles



 */
