package example

import org.apache.spark.sql.SparkSession

import java.util
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object Hello extends App {
  println("First")

  System.setProperty("hadoop.home.dir", "D:\\software\\hadoop")

  val spark = SparkSession.builder().appName("first").master("local[*]").getOrCreate()

  val numberOfWrites = 2000

  var futures: util.List[Future[Boolean]] = new util.ArrayList[Future[Boolean]]()
  var futuresSuccess: Integer = 0
  var futuresFailure: Integer = 0

  for(i <- 1 to numberOfWrites) {
    val future = Future {
      val session = spark.newSession()
      val df1 = session.read.csv("D:\\data\\spark_definitive_guide_data\\flight-data\\csv\\2015-summary.csv")
      df1.write.csv("C:\\temp\\data\\flight_data_" + i)
//      session.close()
      true
    }

    future.onComplete {
      case Success(value) => futuresSuccess += 1
      case Failure(exception) => {
        futuresFailure += 1
        println(exception)
      }
    }

    futures.add(future)
  }

  while(futuresSuccess + futuresFailure < numberOfWrites) {
    println(s"futuresSuccess: $futuresSuccess - futuresFailure: $futuresFailure")
    Thread.sleep(1000)
  }

  println("All writes completed")

  spark.close()
}
