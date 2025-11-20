package org.cscie88c.spark

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Ingestion {

  // Case class / optional Dataset conversion
  case class Flight(
    year: Int,
    month: Int,
    day_of_month: Int,
    day_of_week: Option[Int],
    fl_date_origin: String,
    origin: String,
    origin_city_name: String,
    origin_state_nm: String,
    dep_time: Option[Int],
    taxi_out: Option[Int],
    wheels_off: Option[Int],
    wheels_on: Option[Int],
    taxi_in: Option[Int],
    cancelled: Int,
    air_time: Option[Int],
    distance: Option[Int],
    weather_delay: Option[Int],
    late_aircraft_delay: Option[Int],
    total_delay: Option[Int]
  )

  /** Import CSV as DataFrame */
  def dataImport()(implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/data/flight_data_2024.csv")
  }

  /** Show totals BEFORE cleaning */
  def summaryBronze(flightsDF: DataFrame): Unit = {
    println("=== Totals BEFORE cleaning ===")
    flightsDF.agg(
      count("*").as("total_rows"),
      sum("taxi_out").as("sum_taxi_out"),
      sum("air_time").as("sum_air_time"),
      sum("distance").as("sum_distance")
    ).show()
  }

  /** Show totals AFTER cleaning */
  def summarySilver(flightsDF: DataFrame): Unit = {
    println("=== Totals AFTER cleaning ===")
    flightsDF.agg(
      count("*").as("total_rows"),
      sum("taxi_out").as("sum_taxi_out"),
      sum("air_time").as("sum_air_time"),
      sum("distance").as("sum_distance")
    ).show()
  }

  /** Drop null records and add total_delay column */
  def cleanFlights(flightsDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    flightsDF
    .na.drop(Seq("year", "month", "day_of_month", "origin", "dep_time", "air_time", "distance"))
    .withColumn(
      "total_delay",
      coalesce(col("weather_delay"), lit(0)) + coalesce(col("late_aircraft_delay"), lit(0))
    )
  }

  /** Remove impossible values but keep cancelled flights */
  def removeImpossibleValues(flightsDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    flightsDF.filter(
      (col("taxi_out") >= 0 || col("cancelled") === 1) &&
      (col("taxi_in")  >= 0 || col("cancelled") === 1) &&
      (col("air_time") >= 0 || col("cancelled") === 1) &&
      (col("distance") >= 0 || col("cancelled") === 1)
    )
  }

  /** Convert DataFrame back to Dataset[Flight] */
  def toDataset(flightsDF: DataFrame)(implicit spark: SparkSession): Dataset[Flight] = {
    import spark.implicits._
    flightsDF.as[Flight]
  }

}

/*
[-- Header List --]
year
month
day_of_month
day_of_week
fl_date
origin
origin_city_name
origin_state_nm
dep_time
taxi_out
wheels_off
wheels_on
taxi_in
cancelled
air_time
distance
weather_delay
late_aircraft_delay
*/

// Same main run
// val rawDF = Ingestion.dataImport()
// val cleanedDF = Ingestion.dataCleanup(rawDF)
// val finalDF = Ingestion.removeImpossibleValues(cleanedDF)
