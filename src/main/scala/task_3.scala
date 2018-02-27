

object task_3 {

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    // disable logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // initialize Spark
    val conf = new SparkConf().setAppName("task_3").setMaster("local")
    val sc = new SparkContext(conf)

    // set result file
    val resultFile = "result_3.tsv"
    val resultDirectory = resultFile.replace(".tsv", "")
    ResultManager.deletePreviousResult(resultDirectory)

    /*
      Task 3: For each country that has more than 10 tweets, find its geographical centroid.
        (a) Write a code (named “task_3”) that outputs in a TSV file the latitude and longitude
        of the centroids and the names of the countries, in the form of <country_name>tab<latitude>tab<longitude>
        (b) Visualize the results in CartoDB
        (more information below)
     */

    // load tweets
    var geotweets = sc.textFile("data/geotweets.tsv")

    def makeTuple(line: String): (String, ((Double, Double), Int)) = {
      val splitted = line.split("\t")
      (splitted(1), ((splitted(11).toDouble, splitted(12).toDouble), 1))
    }

    def reduceTuple(tuple1: ((Double, Double), Int), tuple2: ((Double, Double), Int)): ((Double, Double), Int) = {
      val latitudeSum = tuple1._1._1 + tuple2._1._1
      val longitudeSum = tuple1._1._2 + tuple2._1._2
      val tweetCountPerCountry = tuple1._2 + tuple2._2
      ((latitudeSum, longitudeSum), tweetCountPerCountry)
    }

    def calculateAverage(tuple: (String, ((Double, Double), Int))): (String, Double, Double) = {
      val avgLat = tuple._2._1._1 / tuple._2._2
      val avgLong = tuple._2._1._2 / tuple._2._2
      (tuple._1, avgLat, avgLong)
    }

    val res = geotweets.map(makeTuple) // (<country>, ((<lat>, <long>), 1))
      .reduceByKey((tuple1, tuple2) => reduceTuple(tuple1, tuple2)) // (<country>, ((<latSum>, <longSum>), <tweetCount>))
      .filter(tuple => tuple._2._2 > 10) // remove countries with less than 10 tweets
      .map(tuple => calculateAverage(tuple)) // calculate average lat and long
      .map(tuple => tuple._1 + "\t" + tuple._2 + "\t" + tuple._3)

    res.coalesce(1).saveAsTextFile(resultDirectory)
    ResultManager.moveResult(resultDirectory)
  }

}
