package phase2

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object classify {

  def main(args: Array[String]): Unit = {
    // set defaults
    var trainingPath = "data/phase2Test/example_training.tsv"
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
      println("No arguments, stopping program.")
      return
    }

    // disable logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // initialize Spark
    val conf = new SparkConf().setAppName("classify").setMaster("local")
    val sc = new SparkContext(conf)

    classify(sc, trainingPath, inputPath, outputPath)
  }

  def classify(sc: SparkContext, trainingPath: String, inputPath: String, outputPath: String): Unit = {
    val training = sc.textFile(trainingPath).persist()
    val input = sc.textFile(inputPath).persist()

    // get each of the words from the input
    val inputWords = input.flatMap(_.toLowerCase.split(" ")).persist()
    val inputWordsDistinctKVP = inputWords.map(word => (word, 1)).reduceByKey(_ + _) // make a key value pair so we can join it with other pair RDDs

    // calculate necessary counts
    val inputWordCount = inputWords.count()
    val inputDistinctWordCount = inputWords.distinct().count()
    val tweetCount = training.count() // |T|

    def extractPlaceAndTweettext(line: String): (String, String) = {
      val splitted = line.split("\t")
      val placeName = splitted(4)
      val tweetText = splitted(10).toLowerCase
      (placeName, tweetText)
    }

    // create tuples of the form: (place_name, <product of word frequencies>)
    val placeFreqProduct = training.map(extractPlaceAndTweettext).flatMapValues(text => text.split(" ").distinct) // get all words for all places
      .map({ case (place, word) => (word, place) }).join(inputWordsDistinctKVP) // remove words that are not in the input
      .map({ case (word, (place, inputOccurrences)) => ((place, word, inputOccurrences), 1) }).reduceByKey(_ + _) // count the number of tweets each word occur in
      .map({ case ((place, word, inputOccurrences), freq) => ((place, word), Math.pow(freq, inputOccurrences)) })

      // calculate the product of frequencies and check if a place contains all the words from the input
      .map({ case ((place, word), freq) => (place, (freq, 1)) }).reduceByKey((a, b) => (a._1 * b._1, a._2 + b._2))
      // we now have: (place, (<product of frequencies>, <number of words from 'place'>))
      .filter(_._2._2 == inputDistinctWordCount) // filter out the places that don't have all the words
      .mapValues(_._1) // keep only the product of the frequencies
      .persist()

    // calculate |T_c| for all places
    val tweetCountByPlace = training.map(line => (line.split("\t")(4), 1))
      .join(placeFreqProduct).mapValues(_._1) // remove places that doesn't have all the words
      .reduceByKey(_ + _)


    def getMaxProb(a: (String, Double), b: (String, Double)): (String, Double) = {
      if (a._2 > b._2) {
        a
      } else if (a._2 < b._2) {
        b
      } else {
        (a._1 + "\t" + b._1, a._2) // keep both place names
      }
    }

    val probabilities = placeFreqProduct
      .join(tweetCountByPlace) // now: (place, (<product of frequencies>, |T_c|))
      .map({ case (place, (freqProd, count)) => (place, freqProd.toDouble / (tweetCount * Math.pow(count, inputWordCount - 1))) }) // complete the calculation of bayes
      .persist()

    var resultString = ""
    if (!probabilities.isEmpty()) {
      val res = probabilities.reduce(getMaxProb)
      resultString = res._1 + "\t" + res._2
    }

    Files.write(Paths.get(outputPath), resultString.getBytes(StandardCharsets.UTF_8))
  }
}
