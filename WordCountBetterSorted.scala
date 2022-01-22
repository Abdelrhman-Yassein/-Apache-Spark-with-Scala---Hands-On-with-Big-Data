package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountBetterSorted {
  //Our main function where the action happens
  def main(args: Array[String]){
    //set Log level to print only errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create SparkContext using The local machine
    val sc = new SparkContext("local", "WordCountBetterSorted")

    //load each line of book into an rdd
    val input = sc.textFile("data/book.txt")

    //split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))

    //Normalize everything to Lowercase
    val lowerCaseWords = words.map(x => x.toLowerCase())

    // Count of the occurrences of each word
    val wordCounts = lowerCaseWords.map(x => (x, 1)).reduceByKey((x, y) => x + y)

    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted=wordCounts.map(x=>(x._2,x._1)).sortByKey()

    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- wordCountsSorted){
      val count = result._1
      val word=result._2
      println(s"$word:$count")
    }
  }
}