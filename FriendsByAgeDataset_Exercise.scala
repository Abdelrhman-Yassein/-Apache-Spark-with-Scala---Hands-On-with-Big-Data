package com.sundogsoftware.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._

//Compute the average number of friends by age in a social network.
object FriendsByAgeDataset_Exercise {

  //create case class with schema of fakeFriend.csv
  case class FakeFriends(id: Int, name: String, age: Int, friends: Int)

  //our main function where the action happens
  def main(args: Array[String]) {

    //set the log level to only print Errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create a SparkSession using every core of the local machine
    val spark = SparkSession.builder().appName("FriendsByAge").master("local[*]").getOrCreate()

    //Load each line of the source data in dataset

    import spark.implicits._
    val ds = spark.read.option("header", "true").option("inferschema", "true")
      .csv("data/fakefriends.csv").as[FakeFriends]

    //select only age and numFriends columns

    val friendsByAge = ds.select("age", "friends")
    // From friendsByAge we group by "age" and then compute average
    friendsByAge.groupBy("age").avg("friends").show()

    //Sorted
    friendsByAge.groupBy("age").avg("friends").sort("age").show()

    // Formatted more nicely:
    friendsByAge.groupBy("age").agg(round(avg("age"), 2)).sort("age").show()

    //with a custom column name
    friendsByAge.groupBy("age").agg(round(avg("age"), 2).alias("avgFriends"))
      .sort("age").show()


  }
}
