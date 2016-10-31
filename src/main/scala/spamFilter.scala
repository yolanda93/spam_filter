import org.apache.spark.rdd.RDD._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object spamFilter {

  def probaWordDir(sc: SparkContext)(filesDir: String): (RDD[(String, Double)], Long) = {
    println("Call to probaWordDir with args" + filesDir)
    // wholeTextFiles get back a pair RDD where the key is the name of the input file.
    val rdd = sc.wholeTextFiles(filesDir)
    // The number of files is counted and stored in a variable nbFiles
    val nbFiles = rdd.count()
    // Non informative words must be removed from the set of unique words. We have also added ! and ?
    val stopWords = Set(".", ":", ",", " ", "/", "\\", "-", "'", "(", ")", "@")
    // Each text file must be splitted into a set of unique words (if a word occurs several times, it is saved only one time in the set).
    val wordBagRdd: RDD[(String, Set[String])] = rdd.map(textTuple => (textTuple._1, textTuple._2.trim().split("\\s+").toSet.diff(stopWords)))
    // Get the Number of occurrences amongst all files
    val wordCountRdd: RDD[(String, Int)] = wordBagRdd.flatMap(x => x._2.map(y => (y, 1))).reduceByKey(_ + _)
    val probaWord: RDD[(String, Double)] = wordCountRdd.map(x => (x._1, x._2.toDouble / nbFiles))
    (probaWord, nbFiles)
  }

  def computeMutualInformationFactor(
    probaWC: RDD[(String, Double)],
    probaW: RDD[(String, Double)],
    probaC: Double,
    probaDefault: Double // default value when a probability is missing
  ): RDD[(String, Double)] = {
    val probWJoin: RDD[(String, (Double, Option[Double]))] = probaW.leftOuterJoin(probaWC)
    val valueClassAndOcu: RDD[(String, (Double, Double))] = probWJoin.map(x => (x._1, (x._2._1, x._2._2.getOrElse(probaDefault))))
    //We have to change ln to log2 (by using ln(x)/ln(2)=log2(x)
    valueClassAndOcu.map(x => (x._1, x._2._2 * (math.log(x._2._2 / (x._2._1 * probaC)) / math.log(2.0))))
  }

  def main(args: Array[String]) {
    if (args.length > 0)
      println("Started Spam Filter with arg = " + args(0))
    else
      println("You should provide the main directory path ")

    val conf = new SparkConf().setAppName("Spam Filter Application").setMaster("local")
    val sc = new SparkContext(conf)
    val (probaW, nbFiles) = probaWordDir(sc)(args(0) + "/*/*.txt")
    // Compute the couples (probaWordHam, nbFilesHam) for the directory “ham” and (probaWordSpam, nbFilesSpam) for the directory “spam”
    val (probaWordH, nbFilesH) = probaWordDir(sc)(args(0).concat("/ham/*.txt"))
    val (probaWordS, nbFilesS) = probaWordDir(sc)(args(0).concat("/spam/*.txt"))
    //Compute the probability P(occurs, class) for each word.
    // RDD with the map structure: word => probability the word occurs (or not) in an email of a given class.
    val probaWC = (probaWordH, probaWordS, probaWordH.map(x => (x._1, 1 - x._2)), probaWordS.map(x => (x._1, 1 - x._2)))
    val probaH = nbFilesH.toDouble / nbFiles.toDouble // the probability that an email belongs to the given class.
    val probaS = nbFilesS.toDouble / nbFiles.toDouble
    // Compute mutual information for each class and occurs
    val MITrueHam = computeMutualInformationFactor(probaWC._1, probaW, probaH, 0.2 / nbFiles) // the last is a default value
    val MITrueSpam = computeMutualInformationFactor(probaWC._2, probaW, probaS, 0.2 / nbFiles)
    val MIFalseHam = computeMutualInformationFactor(probaWC._3, probaW, probaH, 0.2 / nbFiles)
    val MIFalseSpam = computeMutualInformationFactor(probaWC._4, probaW, probaS, 0.2 / nbFiles)

    //compute the mutual information of each word as a RDD with the map structure: word => MI(word)
    val MI :RDD[(String, Double)] = MITrueHam.union(MITrueSpam).union(MIFalseHam).union(MIFalseSpam).reduceByKey( (x, y) => x + y)

    // print on screen the 10 top words (maximizing the mutual information value)
    //These words must be also stored on HDFS in the file “/tmp/topWords.txt”.
    val path: String = "/tmp/topWords.txt"
    val topTenWords: Array[(String, Double)] = MI.top(10)(Ordering[Double].on(x => x._2))
    //coalesce to put the results in a single file
    sc.parallelize(topTenWords).keys.coalesce(1, true).saveAsTextFile(path)
  }
}
