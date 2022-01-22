package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._

object DataFramesDataset {
  case class Person(id: Int, name: String, age: Int, friends: Int)

  //Our main function where the action happens
  def main(args: Array[String]) {
    //set the log to only print the error
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create new SparkSession
    val spark = SparkSession
      .builder()
      .appName("sparkSql")
      .master("local[*]")
      .getOrCreate()

    //Convert our csv file to a dataset using our person case
    //class to infer the schema
    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    // There are lots of other ways to make a DataFrame.
    // For example, spark.read.json("json file path")
    // or sqlContext.table("Hive table name")

    println("here is our inferred schema")
    people.printSchema()

    println("let's select the name column")
    people.select("name").show()

    println("Filter out anyone over 21 :")
    people.filter(people("age") < 21).show

    println("Group By Age")
    people.groupBy("age").count().show()

    println("make everyone 10 years older:")
    people.select(people("name"), people("age") + 10).show()

    spark.stop()



  }

}
