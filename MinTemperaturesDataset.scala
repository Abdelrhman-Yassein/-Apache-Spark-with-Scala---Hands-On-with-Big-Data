package com.sundogsoftware.spark

import org.apache.spark.sql.types.{FloatType, StringType, IntegerType, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._

/** Find the minimum temperature by weather station */
object MinTemperaturesDataset {
  case class Temperature(stationID: String, date: Int, measure_type: String, temperature: Float)

  //Our main function where the action happens
  def main(args: Array[String]) {

    //Set the log level to only print error
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create SparkSession using every cor of the local machine
    val spark = SparkSession
      .builder
      .appName("MinTemperatures")
      .master("local[*]")
      .getOrCreate()

    val temperatureSchema = new StructType()
      .add("stationID", StringType, nullable = true)
      .add("date", IntegerType, nullable = true)
      .add("measure_type", StringType, nullable = true)
      .add("temperature", FloatType, nullable = true)


    //Read the file as dataset
    import spark.implicits._
    val ds = spark.read
      .schema(temperatureSchema)
      .csv("data/1800.csv")
      .as[Temperature]

    //filter out all but TMIN entries
    val minTemp = ds.filter($"measure_type" === "TMIN")

    //select only stationID ,temperature
    val sectionTemps = minTemp.select("stationID", "temperature")

    //Aggregate to find minimum temperature for every station
    val minTempsByStation = sectionTemps.groupBy("stationId").min("temperature")

    // Convert temperature to fahrenheit and sort the dataset
    val minTempsByStationF = minTempsByStation
      .withColumn("temperature", round($"min(temperature)" * 0.1f * (9.0f / 5.0f) + 32.0f, 2))
      .select("stationID", "temperature").sort("temperature")

    // Collect, Format,and print the result
    val results = minTempsByStationF.collect()

    for (result <- results) {
      val station = result(0)
      val temp = result(1).asInstanceOf[Float]
      val formattedTemp = f"$temp%.2f F"
      println(s"$station minimum temperature: $formattedTemp")
    }
  }
}
