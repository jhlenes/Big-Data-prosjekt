

object task_4 {

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    // disable logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // initialize Spark
    val conf = new SparkConf().setAppName("task_4").setMaster("local")
    val sc = new SparkContext(conf)

    // set result file
    val resultFile = "result_4.tsv"
    val resultDirectory = resultFile.replace(".tsv", "")
    ResultManager.deletePreviousResult(resultDirectory)

    /*
      Calculate local time for each tweet (UTC time + timezone offset) and find the 1-hour
      interval with maximum number of tweets for each country in the form of
      <country_name>tab<begining_hour>tab<tweet_count>. Times should be rounded down
      to the hour, in 24 hour scale, so that a tweet posted between [13:00,14:00) should be
      interpreted as posted at 13. Write a code (named “task_4”) that writes the results in
      a TSV file and name it “result_4.tsv”
     */

    // load tweets
    var geotweets = sc.textFile("data/geotweets.tsv")
    //geotweets = sc.textFile("testdata.txt")

    // convert line to ((<country>, <hour>), <tweet_count>)
    def makeTupleFromLine(line: String): ((String, Int), Int) = {
      val splitted = line.split("\t")
      val country = splitted(1)

      val timezoneOffset = splitted(8).toLong * 1000L // in milliseconds
      val timeInMilliseconds = splitted(0).toLong
      val localTime = new java.util.Date(timeInMilliseconds + timezoneOffset)

      ((country, localTime.getHours), 1)
    }

    val res = geotweets
      .map(makeTupleFromLine) // convert line to ((<country>, <hour>), 1)
      .reduceByKey(_ + _) // sum the tweets for every key: (<country>, <hour>)
      .map({ case (((country, hour), tweetCount)) => (country, (hour, tweetCount)) }) // change the key to be only <country>
      .reduceByKey((tuple1, tuple2) => if (tuple1._2 > tuple2._2) tuple1 else tuple2) // get the hour with the most tweets
      .sortByKey() // sort by country
      .map({ case ((country, (hour, tweetCount))) => country + "\t" + hour + "\t" + tweetCount }) // convert tuple to .tsv string: <country_name>tab<begining_hour>tab<tweet_count>

    res.coalesce(1).saveAsTextFile(resultDirectory) // save to file
    ResultManager.moveResult(resultDirectory) // move results to a .tsv file
  }

}
