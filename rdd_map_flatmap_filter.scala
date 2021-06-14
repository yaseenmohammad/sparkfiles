
import org.apache.spark.sql.SparkSession

object rdd_map_flatmap_filter {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(name = "Spark Application using Scala")
      .master(master = "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val filepath = "file:///C://Users//y256358//Desktop//test.txt"
    val linesRDD = spark.sparkContext.textFile(filepath)
    linesRDD.collect().foreach(println)

    //map
    val wordscountperlineRDD = linesRDD.map(line => line.split(" ").size)
    wordscountperlineRDD.collect().foreach(println)

    //filter
     val lineswithwordfroRDD = linesRDD.filter(line => line.contains("for"))
    lineswithwordfroRDD.collect().foreach(println)

    //flatmap
    val wordRDD = linesRDD.flatMap(line =>line.split(" "))
    wordRDD.collect().foreach(println)
  }

}
