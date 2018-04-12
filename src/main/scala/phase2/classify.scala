package phase2

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object classify {

  def main(args: Array[String]): Unit = {

    // set defaults
    var trainingPath = "data/geotweets.tsv"
    var inputPath = "data/phase2Test/test_input.tsv"
    var outputPath = "data/phase2Test/test_output.tsv"

    // get command line arguments
    if (args.length == 6) {
      var i = 0
      while (i < 6) {
        val arg = args.apply(i)
        if (arg == "-training") {
          trainingPath = args.apply(i + 1)
          i += 2
        } else if (arg == "-input") {
          inputPath = args.apply(i + 1)
          i += 2
        } else if (arg == "-output") {
          outputPath = args.apply(i + 1)
          i += 2
        } else {
          println("Command not recognized: " + arg)
          return
        }
      }
    } else {
      println("Usage:\n\t-training <full path of the training file>\n\t-input <full path of the input file>\n\t-output <full path of the output file>")
      println("No arguments, using defaults.")
    }

    // disable logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // initialize Spark
    val conf = new SparkConf().setAppName("classify").setMaster("local")
    val sc = new SparkContext(conf)

    val training = sc.textFile(trainingPath)
    val input = sc.textFile(inputPath)

    classify(sc, training, input, outputPath)
  }

  def classify(sc: SparkContext, training: RDD[String], input: RDD[String], outputPath: String): Unit = {

    // get each of the words from the input
    val inputWords = input.flatMap(_.toLowerCase.split(" "))

    // calculate necessary counts
    val inputWordCount = inputWords.count()
    val inputDistinctWordCount = inputWords.distinct().count()
    val tweetCount = training.count()

    // create tuples of the form: (place_name, tweet_count)
    val tweetCountByPlace = training.map(line => (line.split("\t")(4), 1)).reduceByKey(_ + _)

    def extractRelevantFields(line: String): (String, String) = {
      val splitted = line.split("\t")
      val placeName = splitted(4)
      val tweetText = splitted(10).toLowerCase
      (placeName, tweetText)
    }

    // create tuples of the form: (word, (place_name, word_frequency))
    val placeWordCount = training.map(extractRelevantFields).flatMapValues(text => text.split(" ").distinct) // (place_name, word). We use distinct because we count how many tweets contain the word, not occurrences of the word
      .map({ case (place, word) => ((place, word), 1) }).reduceByKey(_ + _)
      .map({ case ((place, word), freq) => (word, (place, freq)) })

    def getMaxProb(a: (String, Double), b: (String, Double)): (String, Double) = {
      if (a._2 > b._2) {
        a
      } else if (a._2 < b._2) {
        b
      } else {
        // keep both place names
        (a._1 + "\t" + b._1, a._2)
      }
    }

    val probabilities = inputWords

      // combine the input words with place names and their frequencies from that place
      .map(word => (word, None))
      .cogroup(placeWordCount)
      .filter({ case (key, (a, b)) => a.nonEmpty && b.nonEmpty }) // keep only words that exist both in the input and in a place
      .map({ case (key, (a, b)) => (key, b.toArray) })
      .flatMapValues(identity(_)) // we now have: (word, (place, freq)) if they exist, else nothing

      .map({ case (word, (place, freq)) => (place, (freq, 1)) }) // we don't need the word anymore, so we remove it
      .reduceByKey((a, b) => (a._1 * b._1, a._2 + b._2)) // we now have: (place, (<product of frequencies>, <number of words from 'place'>))

      .filter(_._2._2 == inputDistinctWordCount) // filter out the places that don't have all the words TODO: IS THIS WRONG?
      .mapValues(_._1) // keep only the product of the frequencies. now: (place, <product of frequencies>)

      // get the tweet count for each place
      .cogroup(tweetCountByPlace).filter({ case (key, (a, b)) => a.nonEmpty && b.nonEmpty })
      .map({ case (place, (freqProdIter, countIter)) => (place, freqProdIter.toArray.apply(0), countIter.toArray.apply(0)) })
      // now: (place, <product of frequencies>, <tweet count from 'place'>)

      // complete the calculation of bayes
      .map({ case (place, freqProd, count) => (place, freqProd.toDouble / (tweetCount * Math.pow(count, inputWordCount - 1))) })

    var resultString = ""
    if (!probabilities.isEmpty()) {
      val res = probabilities
        .sortBy(-_._2) // sort by probability
        .reduce(getMaxProb)

      resultString = res._1 + "\t" + res._2
    }

    println(resultString)
    Files.write(Paths.get(outputPath), resultString.getBytes(StandardCharsets.UTF_8))
  }
}
