
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object dataframe_withCoumn_withcolunmrenamed {

  def main(args: Array[String]): Unit = {
    println("Application Started .....")
    val spark = SparkSession
      .builder()
      .appName(name = "Spark DataFrame")
      .master(master = "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


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
    val users_df = spark.createDataFrame(spark.sparkContext.parallelize(users_seq),users_schema)   //rdd,schema
    users_df.show(numRows = 10, truncate = false)
    users_df.printSchema()

    val orders_seq = Seq(
      Row(1 ,"visa" ,"Appliances" ,112.15 ,"2017-05-12 08:30:25 " , 3),
      Row(2 ,"Maestro" ,"elctronics" ,1462.15 ,"2016-05-12 08:30:25 " , 6),
      Row(3 ,"visa" ,"computer" ,143.15 ,"2018-05-12 08:30:25 " , 5),
      Row(4 ,"Mastercard" ,"electronics" ,815.15 ,"2019-05-12 08:30:25 " , 5),
      Row(5 ,"maestro" ,"garden & outdoor" ,54.15 ,"2021-05-12 08:30:25 " , 1))

    val orders_schema = StructType(Array(
      StructField("order_id ", IntegerType),
      StructField("card_type", StringType),
      StructField("product_catergory", StringType),
      StructField("order_amount", DoubleType),
      StructField("order_datetime", StringType),
      StructField("user_id", IntegerType)
    ))

    val orders_df = spark.createDataFrame(spark.sparkContext.parallelize(orders_seq),orders_schema)
    orders_df.show(numRows = 10, truncate = false)
    orders_df.printSchema()

    /*println("Example 1 :")
    users_df.withColumn(colName = "gender", lit(literal = "male")).select(col = "*").show()

    println("example :2")
    orders_df.withColumn(colName = "reference_no" , lit(literal = 100)).select(col = "*").show()

    println("example:3")
    orders_df.withColumn(colName = "order_amount_with_tax",orders_df("order_amount")+10).select("*").show()


    //renamed exercises
    println("example 11")
    val  orders_df_withcolumn  = orders_df.withColumn(colName = "reference_no" , lit(literal = 100)).select(col = "*")
    orders_df_withcolumn.show()

    println("example:22")
    val orders_df_renamed = orders_df_withcolumn.withColumnRenamed(existingName = "reference_no" ,newName = "ref_no").select("*" )
    orders_df_renamed.show()

    println("example :33")
    val orders_filter_df =  orders_df_renamed.filter(conditionExpr = "order_type = 'visa' ")
    orders_filter_df.show()
    //explain
    orders_filter_df.explain()

    println("exapmle : drop ")
    val orders_drop = orders_filter_df.drop(colName = "ref_no")
    orders_drop.show()

    orders_drop.explain()   //physical plan
    orders_drop.describe().show()     //basic statiscis about diff colum in DF min ,max and standard deviation it will show
    println(orders_drop.head())                 //fetch the data frame


    //union    -- combine all data
    //intersect   ---matches the data
    // except    ---which is not present in the df1 =/ df2
     */

        //groupby and agg
     val orders_group1 = orders_df.groupBy(col1 = "card_type")
     println(orders_group1.getClass())
     println(orders_group1.toString())

    val orders_group2 = orders_df.groupBy(col1 = "card_type" , cols = "product_catergory" )
    println(orders_group2.getClass())
    println(orders_group2.toString())

    orders_df.select(col = "card_type").distinct().show()

    //orders_df.groupBy(col1 = "card_type").agg(count(columnName = "order_id")).show ()










    }
}
