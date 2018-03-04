object task_5 {


  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    // disable logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // initialize Spark
    val conf = new SparkConf().setAppName("task_5").setMaster("local")
    val sc = new SparkContext(conf)

    // set result file
    val resultFile = "result_5.tsv"
    val resultDirectory = resultFile.replace(".tsv", "")
    ResultManager.deletePreviousResult(resultDirectory)

    /*
      Task 5: Find the number of tweets from each city in US (place_type = ’city’ and country_code
      = ’US’) in the form of <place_name>tab<tweet_count>. Write a code
      (named “task_5” that writes the results in a TSV file named “result_5.tsv” in descending
      order of tweet counts. For cities with equal number of tweets, sorting must
      be in alphabetical order.
     */

    // load tweets
    var geotweets = sc.textFile("data/geotweets.tsv")

    def makeTupleFromLine(line: String): (((String, String, String))) = {
      val splitted = line.split("\t")
      val countryCode = splitted(2)
      val placeType = splitted(3)
      val placeName = splitted(4)
      (countryCode, placeType, placeName)
    }

    val res = geotweets.map(makeTupleFromLine) //(countryCode, placeType, placeName)
      .filter({ case (countryCode, placeType, placeName) => countryCode == "US" && placeType == "city" })
      .map({ case (countryCode, placeType, placeName) => (placeName, 1) })
      .reduceByKey((a, b) => a + b) //count the tweets
      .sortBy({ case (placeName, tweetCount) => (-tweetCount, placeName) }) // sort by tweet count in descending order, place name alphabetically
      .map(tuple => tuple._1 + "\t" + tuple._2) // to .tsv format

    res.coalesce(1).saveAsTextFile(resultDirectory)
    ResultManager.moveResult(resultDirectory)
  }
}
