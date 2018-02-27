

object task_6 {

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    // disable logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // initialize Spark
    val conf = new SparkConf().setAppName("task_6").setMaster("local")
    val sc = new SparkContext(conf)

    // set result file
    val resultFile = "result_6.tsv"
    val resultDirectory = resultFile.replace(".tsv", "")
    ResultManager.deletePreviousResult(resultDirectory)

    /*
      Task 6: Find the 10 most frequent words (in lowercase) and their frequencies from the US,
      excluding the words shorter than 2 characters (length < 2) and the words from the
      stop words file. Write a code (named “task_6”) that writes the results in a TSV file
      named “result_6.tsv” in the form of <word>tab<frequency>.
     */

    def makeTuple(line: String): (String, String) = {
      val splitted = line.split("\t")
      val countryCode = splitted(2)
      val tweetText = splitted(10)
      (countryCode, tweetText)
    }

    // load tweets
    var geotweets = sc.textFile("data/geotweets.tsv")
    //geotweets = sc.textFile("testdata.txt")
    val stopWords = sc.textFile("data/stop_words.txt")

    val res = geotweets.map(makeTuple) // (country, tweetText)
      .filter({ case (countryCode, _) => countryCode == "US" }) // keep only tweets from US
      .flatMap({ case (_, tweetText) => tweetText.split(" ") }) // get all the words
      .filter(_.length >= 2) // remove words with less than 2 characters
      .map(_.toLowerCase)
      .subtract(stopWords)
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(tuple => -tuple._2)
      .zipWithIndex
      .filter { case (_, index) => index < 10 }
      .keys
      .map(tuple => tuple._1 + "\t" + tuple._2)

    res.coalesce(1).saveAsTextFile(resultDirectory) // save to file
    ResultManager.moveResult(resultDirectory) // move results to a .tsv file
  }

}
