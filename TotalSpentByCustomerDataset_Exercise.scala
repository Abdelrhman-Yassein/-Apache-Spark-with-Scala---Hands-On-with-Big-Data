package com.sundogsoftware.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, IntegerType, FloatType, StructType, DoubleType}
import org.apache.log4j._

object TotalSpentByCustomerDataset_Exercise {
  case class CustomerOrders(cust_id: Int, item_id: Int, amount_spent: Double)

  def main(args: Array[String]) {

    //Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("TotalSpentByCustomer")
      .master("local[*]")
      .getOrCreate()

    val customerOrderSchema = new StructType()
      .add("cust_id", IntegerType, nullable = true)
      .add("item_id", IntegerType, nullable = true)
      .add("amount_spent", DoubleType, nullable = true)

    //Read the file as dataset
    import spark.implicits._
    val customerDS = spark.read
      .schema(customerOrderSchema)
      .csv("data/customer-orders.csv")
      .as[CustomerOrders]

    //Get total customer spent
    val totalByCustomer = customerDS.groupBy("cust_id")
      .agg(round(sum("amount_spent"), 2).alias("total_spent")).sort("total_spent")

    //Show the results
    totalByCustomer.show(totalByCustomer.count.toInt)


  }
}
