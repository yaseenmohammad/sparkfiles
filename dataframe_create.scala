
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

//case class user(user_id :Int,user_name:String ,user_city :String )

object dataframe_create {
  def main(args: Array[String]): Unit = {
    println("Application Started .....")
         val spark = SparkSession
                  .builder()
                  .appName(name = "Spark DataFrame")
                  .master(master = "local")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    //1st approach
    val users_seq = Seq(Row(1, "john", "London"),
                        Row(2, "Ravi", "London"),
                        Row(3, "sita", "UK"),
                        Row(4, "ramesh", "Africa"),
                        Row(5, "vase", "sweden"))

    val users_schema = StructType(Array(
      StructField("user_id ", IntegerType),
      StructField("user_name", StringType),
      StructField("user_city", StringType)
    ))

    val users_df_2 = spark.createDataFrame(spark.sparkContext.parallelize(users_seq),users_schema)   //rdd,schema
    users_df_2.show()
    users_df_2.printSchema()



    /*//case Class Approach
    val case_users_seq = Seq(user(1 , "john" , "London"),
        user(2, "Ravi" , "London"),
        user(3 , "sita" , "UK"),
        user(4 , "ramesh" , "Africa"),
        user(5 , "jagu", "sweden")
        )

      val case_users_rdd = spark.sparkContext.parallelize(case_users_seq)
      val case_users_DF  = spark.createDataFrame(case_users_rdd)
      case_users_DF.show(numRows = 5 ,truncate = true)
      */

    spark.stop()
    println("Application Completed.")
  }

}
