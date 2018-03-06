
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object task_8 {

  import org.apache.spark.sql.{Row, SparkSession}
  import org.apache.spark.sql.types._

  def createSparkSessionWithTweets(): SparkSession = {
    // disable logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Init Spark
    val conf = new SparkConf().setAppName("task_8").setMaster("local")
    new SparkContext(conf)
    val spark = SparkSession.builder().appName("task_8").getOrCreate()

    // Load tweets
    val tweetsRDD = spark.sparkContext.textFile("data/geotweets.tsv")

    // Generate the schema
    val schema = new StructType()
      .add("utc_time", LongType, true).add("country_name", StringType, true)
      .add("country_code", StringType, true).add("place_type", StringType, true)
      .add("place_name", StringType, true).add("language", StringType, true)
      .add("username", StringType, true).add("user_screen_name", StringType, true)
      .add("timezone_offset", IntegerType, true).add("number_of_friends", IntegerType, true)
      .add("tweet_text", StringType, true).add("latitude", DoubleType, true).add("longitude", DoubleType, true)

    // Convert records of the RDD to Rows
    val rowRDD = tweetsRDD.map(_.split("\t"))
      .map(x => Row(x(0).toLong, x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8).toInt, x(9).toInt, x(10), x(11).toDouble, x(12).toDouble))

    val tweetsDF = spark.createDataFrame(rowRDD, schema)
    tweetsDF.createOrReplaceTempView("tweets") // give the DF a name

    spark
  }

  def main(args: Array[String]): Unit = {

    val spark = createSparkSessionWithTweets()

    // (a) Number of tweets
    val tweetCount = spark.sql("SELECT count(*) AS tweetCount FROM tweets")
    tweetCount.show

    // (b) Number of distinct users (username)
    val userCount = spark.sql("SELECT count(DISTINCT username) AS userCount FROM tweets")
    userCount.show

    // (c) Number of distinct countries (country name)
    val countryCount = spark.sql("SELECT count(DISTINCT country_name) AS countryCount FROM tweets")
    countryCount.show

    // (d) Number of distinct places (place name)'
    val placeCount = spark.sql("SELECT count(DISTINCT place_name) AS placeCount FROM tweets")
    placeCount.show

    // (e) Number of distinct languages users post tweets
    val languageCount = spark.sql("SELECT count(DISTINCT language) AS languageCount FROM tweets")
    languageCount.show

    // (f) Minimum values of latitude and longitude
    val minLatLong = spark.sql("SELECT min(latitude) AS minLat, min(longitude) AS minLong FROM tweets")
    minLatLong.show

    // (g) Maximum values of latitude and longitude
    val maxLatLong = spark.sql("SELECT max(latitude) AS maxLat, max(longitude) AS maxLong FROM tweets")
    maxLatLong.show

  }

}
