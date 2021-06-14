import org.apache.spark.sql.{SaveMode, SparkSession}

object csv_to_parquet_format {

  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().appName(name = "CSV file to Parquet").master("local").getOrCreate()
    spark.sparkContext.setLogLevel ("ERROR")

    val df = spark.read.format("csv")
      .option("delimiter" ,",")
      .option("header" ,true)
      .option("inferSchema" ,true)
      .option("mode","MALFORMED")
      .load(path = "C:\\Users\\y256358\\Desktop\\SalesJan2009.csv")

    val df1 = df.withColumnRenamed(existingName = "US Zip" ,newName = "ref_no").select("*" )
    df1.show()


    //write it to parquet
    df1.write.option("compression" , "none")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(path = "C:\\Users\\y256358\\Desktop\\parquet_data")

    //reading parquet file
    println("parquet_data")
    spark.read.parquet(path = "C:\\Users\\y256358\\Desktop\\parquet_data")
    spark.close()


    // parquet to json format
   /*
    val parquetDF = spark.read.format ("parquet")
     .option(path = "C:\\Users\\y256358\\Desktop\\parquet_data")

     parquetDF.printSchema()
     parquetDF.show()

       parquetDF.write.format("json")
       .option("compression" , "none")
       .mode(SaveMode.Overwrite)
       .save(path = "C:\\Users\\y256358\\Desktop\\json_data")


  */

  }
}
