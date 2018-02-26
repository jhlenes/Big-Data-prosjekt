object task_1 {

  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import org.apache.log4j.Logger
  import org.apache.log4j.Level

  def main(args: Array[String]): Unit = {
    // disable logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // initialize Spark
    val conf = new SparkConf().setAppName("task_1").setMaster("local")
    val sc = new SparkContext(conf)

    // load tweets
    var geotweets = sc.textFile("data/geotweets.tsv")
    geotweets = geotweets.sample(false, 0.0001, 5)
    //geotweets.saveAsTextFile(".\\testdata.txt")

    // a) How many tweets are there?
    val ones = geotweets.map(_ => 1)
    val tweetCount = ones.reduce((a, b) => a + b)
    printf("tweetCount: %d\n", tweetCount, "\n")

    // b) How many distinct users (username) are there?
    val users = geotweets.map(line => line.split("\t")(6))
    val uniqueUsers = users.distinct()
    val numOfUniqueUsers = uniqueUsers.count()

    printf("Number of unique users: %d\n", numOfUniqueUsers, "\n")

    // c) How many distinct countries (country_name)?

    val countries = geotweets.map(line => line.split("\t")(1))
    val uniqueCountries = countries.distinct()
    val numOfUniqueCountries = uniqueCountries.count()

    printf("Number of unique countries: %d\n", numOfUniqueCountries, "\n")

  }

}
