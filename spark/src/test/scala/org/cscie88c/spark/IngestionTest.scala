package org.cscie88c.spark

import org.cscie88c.spark.Ingestion // needed?
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class IngestionTest extends AnyFunSuite with BeforeAndAfterAll {

  // Stable SparkSession for all tests
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("IngestionSparkTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._  // stable import
  implicit val implicitSpark: SparkSession = spark 

  override def afterAll(): Unit = {
    spark.stop()
  }

  // Sample test data
  val sampleData: Seq[(Int, Int, Int, String, String, String, String, Option[Int], Option[Int], Option[Int], Option[Int], Option[Int], Int, Option[Int], Option[Int], Option[Int], Option[Int])] = Seq(
    // year, month, day, fl_date_origin, origin, origin_city_name, origin_state_nm,
    // dep_time, taxi_out, wheels_off, wheels_on, taxi_in, cancelled, air_time, distance,
    // weather_delay, late_aircraft_delay
    (2024, 1, 1, "2024-01-01", "JFK", "New York", "NY", Some(700), Some(15), Some(715), Some(900), Some(15), 0, Some(120), Some(800), Some(5), Some(10)),
    (2024, 1, 2, "2024-01-02", "LAX", "Los Angeles", "CA", None, Some(20), Some(810), Some(1030), Some(20), 0, Some(140), Some(900), Some(0), Some(5)),
    (2024, 1, 3, "2024-01-03", "ORD", "Chicago", "IL", Some(600), Some(-5), Some(605), Some(800), Some(10), 1, Some(-1), Some(500), None, None)
  )

  val sampleDF: DataFrame = sampleData.toDF(
    "year", "month", "day_of_month", "fl_date_origin", "origin", "origin_city_name", "origin_state_nm",
    "dep_time", "taxi_out", "wheels_off", "wheels_on", "taxi_in", "cancelled", "air_time", "distance",
    "weather_delay", "late_aircraft_delay"
  )

  test("cleanFlights should drop nulls and compute total_delay") {
    val cleanedDF = Ingestion.cleanFlights(sampleDF)
    cleanedDF.show(false)

    // Verify nulls dropped (rows with null year, month, day_of_month, origin, dep_time, air_time, distance)
    assert(cleanedDF.filter($"dep_time".isNull || $"air_time".isNull).count() == 0)

    // Verify total_delay calculation
    val firstRow = cleanedDF.filter($"origin" === "JFK").head()
    assert(firstRow.getAs[Int]("total_delay") == 15) // 5 + 10
  }

  test("removeImpossibleValues should filter negative values but keep cancelled flights") {
    val cleanedDF = Ingestion.cleanFlights(sampleDF)
    val filteredDF = Ingestion.removeImpossibleValues(cleanedDF)
    filteredDF.show(false)

    // All taxi_out, taxi_in, air_time, distance >=0 OR cancelled == 1
    val invalidCount = filteredDF.filter($"taxi_out" < 0 && $"cancelled" === 0).count()
    assert(invalidCount == 0)

    // Cancelled flight with negative taxi_out stays
    val cancelledFlight = filteredDF.filter($"cancelled" === 1).head()
    assert(cancelledFlight.getAs[Int]("cancelled") == 1)
  }

  test("summary functions should run without errors") {
    Ingestion.summaryBronze(sampleDF)
    val cleanedDF = Ingestion.cleanFlights(sampleDF)
    Ingestion.summarySilver(cleanedDF)
  }
}
