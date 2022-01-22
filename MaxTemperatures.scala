package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

import scala.math.max

object MaxTemperatures {

  def paresLine(line: String): (String, String, Float) = {
    val field = line.split(',')
    val stationID = field(0)
    val entryType = field(2)
    val temperature = field(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }

  //our main function where the action happenes

  def main(args: Array[String]) {

    //Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //create s SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxTemperatures")

    //read the each line of the input data
    val lines = sc.textFile("data/1800.csv")

    //convert to (stationID, entryType, temperature) tuples
    val parseLines = lines.map(paresLine)

    //Filter Out all TMAX Enter
    val maxTemps = parseLines.filter(x => x._2 == "TMAX")

    //Convert to (stationID,  temperature)
    val stationTemps = maxTemps.map(x => (x._1, x._3.toFloat))

    // Reduce by stationID retaining the maximum temperature found
    val maxTmpsByStation = stationTemps.reduceByKey((x, y) => max(x, y))

    //collect ,format, and print the result
    val results = maxTmpsByStation.collect()
    for (result <- results.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station minimum temperature: $formattedTemp")
    }

  }

}
