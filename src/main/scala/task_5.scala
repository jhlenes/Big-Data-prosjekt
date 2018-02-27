object task_5 {


  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import org.apache.log4j.Logger
  import org.apache.log4j.Level

  def main(args: Array[String]): Unit = {
    // disable logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // initialize Spark
    val conf = new SparkConf().setAppName("task_2").setMaster("local")
    val sc = new SparkContext(conf)

    // set result file
    val resultFile = "result_5.tsv"
    val resultDirectory = resultFile.replace(".tsv", "")
    ResultManager.deletePreviousResult(resultDirectory)

    // Task 5: Find the number of tweets from each city in US (place_type = ’city’ and country_code
    //      = ’US’) in the form of <place_name>tab<tweet_count>. Write a code
    //      (named “task_5” that writes the results in a TSV file named “result_5.tsv” in descending
    //      order of tweet counts. For cities with equal number of tweets, sorting must
    //      be in alphabetical order.

    def makeTuple(line: String): (((String, String, String))) = {
      val splitted = line.split("\t")
      val countryCode = splitted(2)
      val placeType = splitted(3)
      val placeName = splitted(4)
      (countryCode, placeType, placeName)

    }

    var geotweets = sc.textFile("data/geotweets.tsv")
    val res = geotweets.map(makeTuple) //(countryCode, placeType, placeName)
      .filter({case(countryCode, placeType, placeName) => countryCode=="US" && placeType=="city"})
      .map({case(countryCode, placeType, placeName) => (placeName,1)})
      .reduceByKey((a, b) => a + b) //count the tweets
      .sortBy({case(placeName, tweetCount) => (-tweetCount, placeName)}) //sorting
      .map(tuple => tuple._1 + "\t" + tuple._2)

    res.coalesce(1).saveAsTextFile(resultDirectory)
    ResultManager.moveResult(resultDirectory)
  }
}
