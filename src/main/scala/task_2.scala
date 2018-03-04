

object task_2 {

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    // disable logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // initialize Spark
    val conf = new SparkConf().setAppName("task_2").setMaster("local")
    val sc = new SparkContext(conf)

    // set result file
    val resultFile = "result_2.tsv"
    val resultDirectory = resultFile.replace(".tsv", "")
    ResultManager.deletePreviousResult(resultDirectory)

    /*
      Task 2: Find the total number of tweets posted from each country and sort them in descending
      order of tweet counts. For countries with equal number of tweets, sorting must be in
      alphabetical order. Write a code (named “task_2”) that writes the results in a TSV
      file in the form of <country_name>tab<tweet_count> and name it “result_2.tsv”
     */

    // load tweets
    val geotweets = sc.textFile("data/geotweets.tsv")

    val res = geotweets.map(line => (line.split("\t")(1), 1)) // get the country name from the line
      .reduceByKey(_ + _) // count the number of times the country appears
      .sortBy(tuple => (-tuple._2, tuple._1)) // sort by descending tweet count, then by ascending country name
      .map(tuple => tuple._1 + "\t" + tuple._2) // convert tuple to .tsv format

    res.coalesce(1).saveAsTextFile(resultDirectory)
    ResultManager.moveResult(resultDirectory)
  }

}
