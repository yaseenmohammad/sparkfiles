import org.apache.spark.{SparkConf, SparkContext}


object wordcount_problem {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("FirstApplication")
    val sc = new SparkContext(conf)

    val emp_data = sc.textFile("src\\main\\resources\\sampledata")
    println(emp_data.foreach(println))

    val result = emp_data.flatMap((_.split(" "))).map(words => (words,1)).reduceByKey(_+_)
    result.collect.foreach(println)

    println( result.getNumPartitions)
  }


}
