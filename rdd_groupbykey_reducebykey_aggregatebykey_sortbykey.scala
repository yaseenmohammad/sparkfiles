import org.apache.spark.sql.SparkSession

object rddgroupbykeyreducebykeyaggregatebykeysortbykey {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(name = "Spark Application using Scala")
      .master(master = "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val filepath = "file:///C://Users//y256358//Desktop//rdd_transformation//words_3.txt"
    val wordRDD = spark.sparkContext.textFile(filepath)
    // println("no of partitions in the RDD")
    //println(wordRDD.getNumPartitions)

     val wordkvpairrdd = wordRDD.map(word => (word,1))
     val wordwithcuntRDD = wordkvpairrdd.reduceByKey(_+_)
     wordwithcuntRDD.collect().foreach(println)

    //reduce ---action
    val numberRDD = spark.sparkContext.parallelize(seq = 1 to 10 ,numSlices = 3)
    val result = numberRDD.reduce((a,b) => a+b)
    println(result)

    val result1 =wordRDD.count()
    println(result1)

    wordRDD.take(num = 3).foreach(println)
   }

  }
