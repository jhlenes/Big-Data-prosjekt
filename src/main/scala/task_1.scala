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

    // a) How many tweets are there?
    val ones = geotweets.map(_ => 1)
    val tweetCount = ones.reduce((a, b) => a + b)
    printf("tweetCount: %d\n", tweetCount, "\n")

    // b) How many distinct users (username) are there?
    val users = geotweets.map(line => line.split("\t")(6)) //Looking at the file, it seems that usernames are at 7, while user screen names are at 6.
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

    // e) In how many languages users post tweets?

    val languages = geotweets.map(line => line.split("\t")(5))
    val uniqueLanguages = languages.distinct()
    val numOfUniqueLanguages = uniqueLanguages.count()

    printf("Number of unique languages: %d\n", numOfUniqueLanguages, "\n")

    // f) What is the minimum latitude?
    val latitude = geotweets.map(line => line.split("\t")(11).toDouble)
    val minimumLatitude = latitude.min()

    printf("Minimum latitude: ")
    println(minimumLatitude)


    // g) What is the minimum longitude?
    val longitude = geotweets.map(line => line.split("\t")(12).toDouble)
    val minimumLongitude = longitude.min()
    printf("Minimum longitude: ")
    println(minimumLongitude)

    // h) What is the maximum latitude?

    val maxLatitude = latitude.max()
    printf("Maximum latitude: ")
    println(maxLatitude)

    // i) What is the maximum longitude? NOTE: There'a mistake in the task description ("What != waht")
    val maxLongitude = longitude.max()
    printf("Maximum longitude: ")
    println(maxLongitude)

    // j) What is the average length of a tweet text in terms of characters?

    val tweetTextLength = geotweets.map(line => line.split("\t")(10).length.toLong)
    var i = 0;
    var totalChars = 0;
    val totalCommentLength = tweetTextLength.reduce((a,b)=>(a + b))
    val averageCommentLength = totalCommentLength/tweetCount

    printf("Average length of tweet text: ")
    println(averageCommentLength)

    // k) What is the average length of a tweet text in terms of words?
    val tweetTexts = geotweets.map(line => line.split("\t")(10))
    val tweetWords = tweetTexts.flatMap(line => line.split(" ")) //Inclues stuff like "&amp;"
    val amountOfWords = tweetWords.count()
    val averageWordCount = amountOfWords/tweetCount

    printf("Average words per tweet: ")
    println(averageWordCount)
  }

}
