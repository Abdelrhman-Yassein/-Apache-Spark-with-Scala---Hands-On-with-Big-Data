package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object TotalSpentByCustomer_Exercise {
  //convert  customerId and amount to tuple
  def parseLine(line: String): (Int, Float) = {
    val field = line.split(",")
    val customerID = field(0).toInt
    val amount = field(2).toFloat
    (customerID, amount)
  }

  //Our main function where the action happens
  def main(args: Array[String]): Unit = {
    //set the log level to print only errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create SparkContext on Local Machine
    val sc = new SparkContext("local", "TotalSpentByCustomer")

    //Load input data into RDD
    val input = sc.textFile("data/customer-orders.csv")

    //Apply parseLine Function to input
    val rdd = input.map(parseLine)

    val reduceInput = rdd.reduceByKey((x, y) => x + y)
    val results = reduceInput.collect()
    results.foreach(println)
  }
}
