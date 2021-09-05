package example

import org.apache.spark.sql.SparkSession

import java.util
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object WithExecutor extends App {
  println("Started: " + new Date())

  System.setProperty("hadoop.home.dir", "D:\\software\\hadoop")

  val spark = SparkSession.builder().appName("first").master("local[*]").getOrCreate()

  val numberOfWrites = 10000

  var futures: util.List[Future[Int]] = new util.ArrayList[Future[Int]]()
  val futuresSuccess: AtomicInteger = new AtomicInteger()
  val futuresFailure: AtomicInteger = new AtomicInteger()

  for(i <- 1 to numberOfWrites) {
    val future = Future {
      try {
              val session = spark.newSession()
        val df1 = session.read.csv("D:\\data\\spark_definitive_guide_data\\flight-data\\csv\\2015-summary.csv")
        df1.write.csv("D:\\temp\\data\\flight_data_" + i + "_" + System.currentTimeMillis())
      } catch {
        case e: Exception=>
          futuresFailure.incrementAndGet()
          println("Failed#1: ")
          e.printStackTrace
      }
      i
    }

    futures.add(future)
  }

  val it = futures.iterator()
  while(it.hasNext) {
    val future = it.next()
    future.onComplete {
      case Success(value) =>
        futuresSuccess.incrementAndGet()
        println(s"Completed Job: $value")
      case Failure(exception) =>
        futuresFailure.incrementAndGet()
        println("Failed#2")
        exception.printStackTrace
    }
  }

  while((futuresSuccess.get() + futuresFailure.get()) < numberOfWrites) {
    println(s"futuresSuccess: $futuresSuccess - futuresFailure: $futuresFailure")
    Thread.sleep(1000)
  }

  println("All writes completed")

  spark.close()

  println("Completed: " + new Date())
}
