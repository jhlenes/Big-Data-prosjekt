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

    // d) How many distinct places (place_name) are there?

    val places = geotweets.map(line => line.split("\t")(4))
    val uniquePlaces = places.distinct()
    val numOfUniquePlaces = uniquePlaces.count()

    printf("Number of unique places: %d\n", numOfUniquePlaces, "\n")

    //places.foreach(println)

    // e) In how many languages users post tweets?

    val languages = geotweets.map(line => line.split("\t")(5))
    val uniqueLanguages = languages.distinct()
    val numOfUniqueLanguages = uniqueLanguages.count()

    printf("Number of unique languages: %d\n", numOfUniqueLanguages, "\n")

    // f) What is the minimum latitude?
    val latitude = geotweets.map(line => line.split("\t")(11))
    val minimumLatitude = latitude.min()

    printf("Minimum latitude: ")
    println(minimumLatitude)


  }

}
