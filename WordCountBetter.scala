package com.sundogsoftware.spark
import org.apache.spark._
import org.apache.log4j._

// Count up how many of each word occurs in a book, using regular expressions
object WordCountBetter {

  //Our main function where the acton happens
  def main(args:Array[String]): Unit ={

    //set the Log level  to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //create a SparkContext using every core of local machine
    val sc = new SparkContext("local[*]","WordCountBetter")

    //load each line of my book into RDD
    val input=sc.textFile("data/book.txt")

    //split using regular expression that extracts words
    val words = input.flatMap(x=>x.split("\\W+"))

    //Normalize every thing to lowercase
    val lowercaseWords = words.map(x=>x.toLowerCase())

    // Count of the occurrences of each word
    val wordCounts=lowercaseWords.countByValue()

    //print the result
    wordCounts.map(println)

  }

}
