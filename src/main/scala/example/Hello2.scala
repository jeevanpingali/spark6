package example

import org.apache.spark.sql.SparkSession

import java.util.Date

object Hello2 extends App {
  println("Started: " + new Date())

  System.setProperty("hadoop.home.dir", "D:\\software\\hadoop")

  val spark = SparkSession.builder().appName("first").master("local[*]").getOrCreate()

  val numberOfWrites = 100

  for(i <- 1 to numberOfWrites) {
    try {
      val session = spark.newSession()
      val df1 = session.read.csv("D:\\data\\spark_definitive_guide_data\\flight-data\\csv\\2015-summary.csv")
      df1.write.csv("D:\\temp\\data\\flight_data_" + i + "_" + System.currentTimeMillis())
    } catch {
      case e: Exception=>
        println("Failed#1: ")
        e.printStackTrace
    }
  }

  println("All writes completed")

  spark.close()
  println("Completed: " + new Date())
}
