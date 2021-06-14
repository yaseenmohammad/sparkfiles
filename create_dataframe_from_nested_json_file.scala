import org.apache.spark.sql.SparkSession

object create_dataframe_from_nested_json_file {
  def main(args: Array[String]): Unit = {
    println("Application Started .....")
    val spark = SparkSession.builder()
      .appName(name = "Create DataFrame from  Nested Json File")
      .master(master = "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val json_multiline = 	"C:\\Users\\y256358\\Desktop\\transaction.json"
    val user_df_3 = spark.read.option("multiline","true").json(json_multiline)

    user_df_3.createOrReplaceTempView(viewName = "df")
    spark.sql(("select profit_center,total_cost,tp.*,tt.* from df lateral view explode(transaction_product) as tp " +
      " lateral view explode(transaction_terms) as tt")).show()
    spark.stop()
  }
}
