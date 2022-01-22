package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.math.min

object MinTemperatures {
  def parseLine(line: String): (String, String, Float) = {
    val fields = line.split(',')
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }

  //Our the main function where the action happens
  def main(args: Array[String]) {

    //Set The LogLevel To Only Print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create a SparkContext using every core Of Local Machine
    val sc = new SparkContext("local[*]", "MinTemperatures")

    //Read the each line of input data
    val lines = sc.textFile("data/1800.csv")

    // Convert to (stationID, entryType, temperature) tuples
    val parsedLine = lines.map(parseLine)

    //Filter Out all TMIN entries
    val minTamp = parsedLine.filter(x => x._2 == "TMIN")

    //// Convert to (stationID, temperature)
    val stationTemps = minTamp.map(x => (x._1, x._3.toInt))

    // Reduce by stationID retaining the minimum temperature found
    val minTempsByStation = stationTemps.reduceByKey((x, y) => min(x, y))

    // Collect, format, and print the results
    val results = minTempsByStation.collect()

    for (result <- results.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station minimum temperature: $formattedTemp")
    }
  }
}
