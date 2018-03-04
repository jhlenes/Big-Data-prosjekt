object task_7 {

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    val t0 = System.currentTimeMillis()

    // disable logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // initialize Spark
    val conf = new SparkConf().setAppName("task_7").setMaster("local")
    val sc = new SparkContext(conf)

    // set result file
    val resultFile = "result_7.tsv"
    val resultDirectory = resultFile.replace(".tsv", "")
    ResultManager.deletePreviousResult(resultDirectory)

    /*
      Task 7: Find the 5 cities in the US with the highest number of tweets (place_type = ’city’ and
      country_code = ’US’, ordered by their tweet counts/alphabetical). For these 5 cities,
      find the 10 most frequent words ordered by their frequency, ignoring the stop words
      from the file and excluding words shorted than 2 characters (length < 2). Write a code
      (named “task_7”) that writes the results in a TSV file named “result_7.tsv” in the
      form of <place_name>tab<word1>tab<frequency1>tab<word2>tab<frequency2> ...
      <word10>tab<frequency10>.
     */

    val geotweets = sc.textFile("data/geotweets.tsv")
    val stopWords = sc.textFile("data/stop_words.txt").map((_, 1))

    def makeTupleFromLine(line: String): (String, String, String) = {
      val splitted = line.split("\t")
      val countryCode = splitted(2)
      val placeType = splitted(3)
      val placeName = splitted(4)
      (countryCode, placeType, placeName)
    }

    def makeTuple(line: String): (String, String) = {
      val splitted = line.split("\t")
      val placeName = splitted(4)
      val tweetText = splitted(10)
      (placeName, tweetText)
    }

    def toTsv(tuple: (String, List[(String, Int)])): String = {
      val res = new StringBuilder(tuple._1)
      tuple._2.foreach({ case (word, count) => res.append("\t" + word + "\t" + count) })
      res.toString()
    }

    val topFiveCities = geotweets.map(makeTupleFromLine)
      .filter({ case (countryCode, placeType, _) => countryCode == "US" && placeType == "city" }) // keep only US cities
      .map({ case (_, _, placeName) => (placeName, 1) })
      .reduceByKey(_ + _) // count tweets per place
      .sortBy(tuple => (-tuple._2, tuple._1)) // descending count, alphabetical name
      .map(tuple => tuple._1).collect().take(5) // get the top five cities

    val res = geotweets.map(makeTuple)
      .filter(tuple => topFiveCities.contains(tuple._1)) // keep only top five cities
      .flatMapValues(tweetText => tweetText.split(" ")) // get the words
      .filter({ case (_, word) => word.length >= 2 }) // remove short words
      .map({ case (place, word) => (word.toLowerCase, place) }).subtractByKey(stopWords).map(_.swap) // remove stop words
      .map({ case (place, word) => ((place, word), 1) }).reduceByKey(_ + _) // count the words
      .map({ case ((place, word), count) => (place, (word, count)) }) // restructure KV pair
      .mapValues(List(_)).reduceByKey(_ ++ _) // combine (word, count) KV pairs in a list
      .mapValues(_.sortBy({ case (word, count) => (-count, word) }).take(10)) // take top 10 words
      .sortBy({ case (city, _) => topFiveCities.indexOf(city) }) // sort places by total tweets
      .map(toTsv)

    res.coalesce(1).saveAsTextFile(resultDirectory) // save to file
    ResultManager.moveResult(resultDirectory) // move results to a .tsv file

    println("Time: " + (System.currentTimeMillis() - t0))
  }
}
