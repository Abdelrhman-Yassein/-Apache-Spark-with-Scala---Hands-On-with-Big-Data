package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, IntegerType, StructType}

/** Find the movies with the most ratings. */
object PopularMoviesDataset {
  // Case class so we can get a column name for our movie ID
  final case class Movie(movieID: Int)

  //Our main function where the action happens
  def main(args: Array[String]) {

    //Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Use SparkSession in Spark
    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()

    //Create Schema when reading u.data
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    //Load up movie data as dataset
    import spark.implicits._
    val movieDS = spark
      .read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Movie]

    // Some SQL-style magic to sort all movies by popularity in one line!
    val topMovieIDs = movieDS.groupBy("movieID").count().orderBy(desc("count"))

    //grab The top 10
    topMovieIDs.show(10)

    //Stop Spark
    spark.stop()

  }

}
