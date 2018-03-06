import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object task_1 {

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    // disable logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // initialize Spark
    val conf = new SparkConf().setAppName("task_1").setMaster("local")
    val sc = new SparkContext(conf)

    val resultString = new StringBuilder()

    // load tweets
    val geotweets = sc.textFile("data/geotweets.tsv")

    // a) How many tweets are there?
    val tweetCount = geotweets.count()

    resultString.append(tweetCount).append("\n")

    // b) How many distinct users (username) are there?
    val users = geotweets.map(line => line.split("\t")(6))
    val uniqueUsersCount = users.distinct().count()

    resultString.append(uniqueUsersCount).append("\n")

    // c) How many distinct countries (country_name)?
    val countries = geotweets.map(line => line.split("\t")(1))
    val uniqueCountriesCount = countries.distinct().count()

    resultString.append(uniqueCountriesCount).append("\n")

    // d) How many distinct places (place_name) are there?
    val places = geotweets.map(line => line.split("\t")(4))
    val uniquePlacesCount = places.distinct().count()

    resultString.append(uniquePlacesCount).append("\n")

    // e) In how many languages users post tweets?
    val languages = geotweets.map(line => line.split("\t")(5))
    val uniqueLanguagesCount = languages.distinct().count()

    resultString.append(uniqueLanguagesCount).append("\n")

    // f) What is the minimum latitude?
    val latitude = geotweets.map(line => line.split("\t")(11).toDouble)
    val minimumLatitude = latitude.min()

    resultString.append(minimumLatitude).append("\n")

    // g) What is the minimum longitude?
    val longitude = geotweets.map(line => line.split("\t")(12).toDouble)
    val minimumLongitude = longitude.min()

    resultString.append(minimumLongitude).append("\n")

    // h) What is the maximum latitude?
    val maxLatitude = latitude.max()

    resultString.append(maxLatitude).append("\n")

    // i) What is the maximum longitude?
    val maxLongitude = longitude.max()
    resultString.append(maxLongitude).append("\n")

    // j) What is the average length of a tweet text in terms of characters?
    val tweetTextLength = geotweets.map(line => line.split("\t")(10).length.toLong)
    val totalTweetTextsLength = tweetTextLength.reduce((a, b) => a + b)
    val averageTweetTextLength = totalTweetTextsLength / tweetCount

    resultString.append(averageTweetTextLength).append("\n")

    // k) What is the average length of a tweet text in terms of words?
    val tweetTexts = geotweets.map(line => line.split("\t")(10))
    val tweetWords = tweetTexts.flatMap(line => line.split(" "))
    val amountOfWords = tweetWords.count()
    val averageWordCount = amountOfWords / tweetCount

    resultString.append(averageWordCount).append("\n")

    // write the result to file
    Files.write(Paths.get("data/result_1.tsv"), resultString.toString().getBytes(StandardCharsets.UTF_8))
  }
}
