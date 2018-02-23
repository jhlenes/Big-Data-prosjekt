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

    val distFile = sc.textFile("data.txt")
    val words = distFile.flatMap(_.split(" "))

    val wordsThatStartWithS = words.filter(s => s.charAt(0) == 's')
    wordsThatStartWithS.foreach(println)

  }

}
