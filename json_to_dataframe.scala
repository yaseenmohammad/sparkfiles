import org.apache.spark.sql.SparkSession

object json_to_dataframe {
  def main(args: Array[String]): Unit = {
    println("Application Started .....")
    val spark = SparkSession.builder()
      .appName(name = "Create DataFrame from Json File")
      .master(master = "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel ("ERROR")

    val json_single = 	"C:\\Users\\y256358\\Desktop\\employees_singleLine.json"
    val user_df_4 = spark.read.json(json_single)
    user_df_4.printSchema()
    user_df_4.show()

    val json_multiline_filepath = 	"C:\\Users\\y256358\\Desktop\\employees_multiLine.json"
    val user_df_2 = spark.read.option("multiline","true").json(json_multiline_filepath)
    user_df_2.printSchema()
    user_df_2.show()

    val json_multiline = 	"C:\\Users\\y256358\\Desktop\\transaction.json"
    val user_df_3 = spark.read.option("multiline","true").json(json_multiline)
    user_df_3.printSchema()
    user_df_3.show()


    spark.stop()
  }

}
