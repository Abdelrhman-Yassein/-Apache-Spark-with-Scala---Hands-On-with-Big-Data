package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

// Count up how many of each word appears in a book as simply as possible.
object WordCount {
  //Our main function where the action happens
  def main(args: Array[String]) {

    //set the Log Level to only Print Error
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create SparkContext Using every core of the local machine
    val sc = new SparkContext("local[*]","WordCount")

    //read each line of My book Into rdd
    val input = sc.textFile("data/book.txt")

    //split into words separator by space
    val word = input.flatMap(x=>x.split(" "))

    // Count up the occurrences of each word
    val wordCounts = word.countByValue()

    //Print The result
    wordCounts.foreach(println)

  }

}
