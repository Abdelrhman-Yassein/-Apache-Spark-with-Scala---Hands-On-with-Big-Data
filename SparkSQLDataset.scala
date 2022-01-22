package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQLDataset {

  case class Person(id: Int, name: String, age: Int, friends: Int)

  //Our Main Function Where The Action happens
  def main(args: Array[String]) {
    //Set the log file to only print the error
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create New Spark Session
    val spark = SparkSession.builder()
      .appName("SparkSql")
      .master("local[*]")
      .getOrCreate()

    //Convert our csv file to a dataset,using our person case
    //class to infer the schema
    import spark.implicits._
    val schemaPeople=spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/fakefriends.csv")
      .as[Person]

    //Print Schema
    schemaPeople.printSchema()

    //Register the Dataframe as a Sql temporary view
    schemaPeople.createTempView("people")

    val teenagers = spark.sql("select * from people where age >=13 and age <=19")

    val results = teenagers.collect()

    results.foreach(println)
    spark.stop()
  }
}
