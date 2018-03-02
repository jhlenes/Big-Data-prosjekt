object task_7 {

  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import org.apache.log4j.Logger
  import org.apache.log4j.Level

  def main(args: Array[String]): Unit = {
    // disable logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // initialize Spark
    val conf = new SparkConf().setAppName("task_7").setMaster("local")
    val sc = new SparkContext(conf)

    // set result file
    val resultFile = "result_5.tsv"
    val resultDirectory = resultFile.replace(".tsv", "")
    ResultManager.deletePreviousResult(resultDirectory)

    /*   Task 7: Find the 5 cities in the US with the highest number of tweets (place_type = ’city’ and
        country_code = ’US’, ordered by their tweet counts/alphabetical). For these 5 cities,
        find the 10 most frequent words ordered by their frequency, ignoring the stop words
          from the file and excluding words shorted than 2 characters (length < 2). Write a code
        (named “task_7”) that writes the results in a TSV file named “result_7.tsv” in the
          form of <place_name>tab<word1>tab<frequency1>tab<word2>tab<frequency2> ...
          <word10>tab<frequency10>.*/

    def makeTuple(line: String): (((String, String, String, String))) = {
      val splitted = line.split("\t")
      val countryCode = splitted(2)
      val placeType = splitted(3)
      val placeName = splitted(4)
      val tweetText = splitted(10)
      (countryCode, placeType, placeName, tweetText)
    }

    var geotweets = sc.textFile("data/geotweets.tsv")
    val stopWords = sc.textFile("data/stop_words.txt")


    val res = geotweets.map(makeTuple) //countryCode, place type, place name, tweet text
      .filter({ case (countryCode, placeType, _, _) => countryCode == "US" && placeType == "city" })
      .map({ case (countryCode, placeType, placeName, tweetText) => (placeName, tweetText) })
      .groupByKey()
      .map({ case (placeName, tweetTexts) => (placeName, tweetTexts, tweetTexts.count(_ => true)) })
      .sortBy(tuple => (-tuple._3, tuple._1))
      .zipWithIndex
      .filter { case (_, index) => index < 5 }
      .keys
      .map(tuple => (tuple._1, tuple._2))
      .flatMapValues(tweetTexts => tweetTexts.flatMap(line => line.split(" ")))
      .filter({ case (_, word) => word.length >= 2 })
      .map(_.swap)
      //.subtractByKey(stopWords)


    //      .map({ case (city, word) => ((city, word), 1) })
    //      .reduceByKey(_ + _)
    //
    //
    //      .foreach(println)

    //      .map(tuple => (tuple._1, tuple._2.flatMap(tweet => tweet.split(" ")))) // (country, [words])
    //      .foreach(println)

    //
    //      .filter(_.length >= 2) // remove words with less than 2 characters
    //      .map(_.toLowerCase)
    //      .subtract(stopWords


    /*
    .flatMap({ case (_, tweetText) => tweetText.split(" ") }) // get all the words
    .filter(_.length >= 2) // remove words with less than 2 characters
    .map(_.toLowerCase)
    .subtract(stopWords)
*/

  }

}
