package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountBetterSortedDataset {

  case class Book(value: String)

  //Our main function where the action happens
  def main(args: Array[String]) {

    //Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create SparkSession using ever core of the local machine
    val spark = SparkSession
      .builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    //read each line from book into dataset
    import spark.implicits._
    val input = spark.read.text("data/book.txt").as[Book]

    //Split using a regular expression that extracts words
    val words = input
      .select(explode(split($"value", "\\W+")).alias("word"))
      .filter($"word" =!= "")

    //Normalize everything to lowercase
    val lowercaseWords = words.select(lower($"word").alias("word"))

    //Count up the occurrences of each word
    val wordCounts = lowercaseWords.groupBy("word").count()

    //Sort By Count
    val wordCountsSorted = wordCounts.sort("count")

    //show the result
    wordCountsSorted.show(wordCountsSorted.count.toInt)

    // ANOTHER WAY TO DO IT (Blending RDD's and Datasets)
    val bookRDD=spark.sparkContext.textFile("data/book.txt")
    val wordsRDD=bookRDD.flatMap(x=>x.split("\\W+"))
    val wordsDS=wordsRDD.toDS()

    val lowercaseWordsDS=wordsDS.select(lower($"value").alias("word"))
    val wordCountDS=lowercaseWords.groupBy("word").count()
    val wordCountsSortedDS=wordCountDS.sort("count")
    wordCountsSorted.show(wordCountsSorted.count.toInt)

  }
}
