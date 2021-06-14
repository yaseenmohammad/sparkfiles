import org.apache.spark.sql.SparkSession

object cmt_commerical_json_data {

  def main(args: Array[String]): Unit = {
    println("cmt commerical of JSON Data to the DataFrame.....")
    val spark = SparkSession.builder()
      .appName(name = "CMT Commerical Transaction json data")
      .master(master = "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val json_multiline = 	"C:\\Users\\y256358\\Desktop\\cmt1.json"
    val user_df_5 = spark.read.option("multiline","true").json(json_multiline)
    user_df_5.show()
    user_df_5.printSchema()

    user_df_5.createOrReplaceTempView(viewName = "df")


   spark.sql(("select changeEventId,entityType, record.salestransaction.last_updated_by_user_identifier, record.salestransaction.last_updated_timestamp," +
     "record.salestransaction.my_order.order_line_number, record.salestransaction.profit_center, record.salestransaction.sales_invoice.created_by_user_identifier," +
     "record.salestransaction.sales_invoice.created_timestamp, record.salestransaction.sales_invoice.delivered_invoice_price, record.salestransaction.sales_invoice.invoice_date," +
     "record.salestransaction.sales_invoice.invoice_line_number, record.salestransaction.sales_invoice.invoice_number, " +
     "record.salestransaction.sales_invoice.last_updated_by_user_identifier, aa.*, ab.*, ac.*, record.salestransaction.sales_order.created_by_user_identifier, " +
     "record.salestransaction.sales_order.created_timestamp, record.salestransaction.sales_order.last_updated_by_user_identifier," +
     "record.salestransaction.sales_order.last_updated_timestamp, record.salestransaction.sales_order.order_date, record.salestransaction.sales_order.order_line_number," +
     "record.salestransaction.sales_order.order_number, chlgs.*, record.salestransaction.sales_pricing.created_by_user_identifier, " +
     "record.salestransaction.sales_pricing.created_timestamp, record.salestransaction.sales_pricing.last_updated_by_user_identifier," +
     "record.salestransaction.sales_pricing.pricing_market_price, ba.*, bb.*, record.salestransaction.sales_shipment.carrier_identifier," +
     "record.salestransaction.sales_shipment.created_timestamp, record.salestransaction.sales_shipment.currency_code," +
     "record.salestransaction.sales_shipment.dellivery_method,record.salestransaction.sales_shipment.last_updated_by_user_identifier," +
     "record.salestransaction.sales_shipment.last_updated_timestamp, record.salestransaction.sales_shipment.ship_from_location," +
     "record.salestransaction.sales_shipment.shipment_date, record.salestransaction.sales_shipment.shipment_market_price," +
     "record.salestransaction.sales_shipment.transportation_means, record.salestransaction.sales_shipment.transportation_mode_description,ca.*, cb.*, cc.*, cd.*," +
     "record.salestransaction.total_cost, tc.*, tcs.*, tp.*, tsr.*, tt.* from df " +
     "lateral view explode(record.salestransaction.sales_invoice.currency_code) as aa " +
     "lateral view explode(record.salestransaction.sales_invoice.fob_invoice_price) as ab " +
     "lateral view explode(record.salestransaction.sales_invoice.last_updated_timestamp) as ac " +
     "lateral view explode(changeLogs) as chlgs " +
     "lateral view explode(record.salestransaction.sales_pricing.last_updated_timestamp) as ba " +
     "lateral view explode(record.salestransaction.sales_pricing.pricing_date) as bb " +
     "lateral view explode(record.salestransaction.sales_shipment.created_by_user_identifier) as ca " +
     "lateral view explode(record.salestransaction.sales_shipment.plant_identifier) as cb " +
     "lateral view explode(record.salestransaction.sales_shipment.shipment_number) as cc " +
     "lateral view explode(record.salestransaction.sales_shipment.transportation_mode_code) as cd " +
     "lateral view explode(record.salestransaction.transaction_cost) as tc " +
     "lateral view explode(record.salestransaction.transaction_customer) as tcs " +
     "lateral view explode(record.salestransaction.transaction_product) as tp " +
     "lateral view explode(record.salestransaction.transaction_source_reference) as tsr " +
     "lateral view explode(record.salestransaction.transaction_terms) as tt")).show()




    /*spark.sql(("select changeEventId,entityType, chlgs.*, tc.*, tcs.*, tp.*, tsr.*, tt.*  from df " +
      "lateral view explode(changeLogs) as chlgs " +
      "lateral view explode(record.salestransaction.transaction_cost) as tc " +
      "lateral view explode(record.salestransaction.transaction_customer) as tcs" +
      " lateral view explode(record.salestransaction.transaction_product) as tp " +
      "lateral view explode(record.salestransaction.transaction_source_reference) as tsr" +
      "lateral view explode(record.salestransaction.transaction_terms) as tt")).show()  */


    spark.stop()
  }
}
