import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Day28BookingStreaming {

  def main(args: Array[String]): Unit = {

    println()
    println("================================================")
    println("      DAY 28 - REAL-TIME BOOKING STREAMING")
    println("================================================")

    val conf = new SparkConf()
      .setAppName("Day28BookingStreaming")
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.ui.showConsoleProgress", "false")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("OFF")
    ssc.checkpoint("/tmp/day28-booking-checkpoint")

    val spark = SparkSession.builder()
      .config(ssc.sparkContext.getConf)
      .getOrCreate()

    import spark.implicits._

    // ------------------------------------------------
    // BROADCAST REFERENCE DATA
    // ------------------------------------------------

    val capacityData = Map(
      "HOTEL_H001" -> 100,
      "FLIGHT_F001" -> 180,
      "BUS_B001" -> 50
    )

    val locationData = Map(
      "HOTEL_H001" -> "Hyderabad",
      "FLIGHT_F001" -> "Delhi",
      "BUS_B001" -> "Bangalore"
    )

    val broadcastCapacity =
      ssc.sparkContext.broadcast(capacityData)

    val broadcastLocation =
      ssc.sparkContext.broadcast(locationData)

    // ------------------------------------------------
    // BOOKING EVENT STREAM
    // ------------------------------------------------

    val lines =
      ssc.socketTextStream("localhost", 9999)

    /*
      Input format:
      bookingId,eventType,serviceId,customerId

      Example:
      BK001,BOOK,HOTEL_H001,C001
      BK002,BOOK,HOTEL_H001,C002
      BK003,CANCEL,HOTEL_H001,C001
    */

    val bookingEvents = lines.flatMap { line =>

      val parts = line.split(",")

      if (parts.length == 4) {
        Some(
          (
            parts(0).trim,
            parts(1).trim.toUpperCase,
            parts(2).trim,
            parts(3).trim
          )
        )
      } else {
        None
      }
    }

    val persistedEvents =
      bookingEvents.persist(StorageLevel.MEMORY_AND_DISK)

    // ------------------------------------------------
    // DISPLAY EVENTS
    // ------------------------------------------------

    persistedEvents.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("================================================")
        println("              BOOKING EVENTS")
        println("================================================")

        rdd.collect().foreach {
          case (bookingId, eventType, serviceId, customerId) =>
            println(
              s"$bookingId | $eventType | $serviceId | $customerId"
            )
        }

        println("================================================")
      }
    }

    // ------------------------------------------------
    // PAIR RDD - BOOKING COUNTS BY SERVICE
    // ------------------------------------------------

    val bookingPairs =
      persistedEvents
        .filter {
          case (_, eventType, _, _) =>
            eventType == "BOOK"
        }
        .map {
          case (_, _, serviceId, _) =>
            (serviceId, 1)
        }

    val bookingCounts =
      bookingPairs.reduceByKey(_ + _)

    bookingCounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("================================================")
        println("          BOOKING COUNTS BY SERVICE")
        println("================================================")

        rdd.collect()
          .sortBy(_._1)
          .foreach {
            case (serviceId, count) =>
              println(
                s"$serviceId -> $count bookings"
              )
          }

        println("================================================")
      }
    }

    // ------------------------------------------------
    // CANCELLATION COUNTS
    // ------------------------------------------------

    val cancellationCounts =
      persistedEvents
        .filter {
          case (_, eventType, _, _) =>
            eventType == "CANCEL"
        }
        .map {
          case (_, _, serviceId, _) =>
            (serviceId, 1)
        }
        .reduceByKey(_ + _)

    cancellationCounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("================================================")
        println("          CANCELLATION COUNTS")
        println("================================================")

        rdd.collect()
          .sortBy(_._1)
          .foreach {
            case (serviceId, count) =>
              println(
                s"$serviceId -> $count cancellations"
              )
          }

        println("================================================")
      }
    }

    // ------------------------------------------------
    // BOOKING / CANCELLATION STATE
    // ------------------------------------------------

    val bookingState =
      persistedEvents
        .map {
          case (_, eventType, serviceId, _) =>
            val change =
              if (eventType == "BOOK") 1
              else if (eventType == "CANCEL") -1
              else 0

            (serviceId, change)
        }
        .filter(_._2 != 0)

    val currentOccupancy =
      bookingState.updateStateByKey(
        (
          newValues: Seq[Int],
          previousState: Option[Int]
        ) => {

          val previous =
            previousState.getOrElse(0)

          Some(
            math.max(
              0,
              previous + newValues.sum
            )
          )
        }
      )

    currentOccupancy.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("================================================")
        println("             CURRENT OCCUPANCY")
        println("================================================")

        rdd.collect()
          .sortBy(_._1)
          .foreach {
            case (serviceId, occupied) =>

              val capacity =
                broadcastCapacity.value
                  .getOrElse(serviceId, 0)

              val available =
                math.max(
                  0,
                  capacity - occupied
                )

              val location =
                broadcastLocation.value
                  .getOrElse(serviceId, "Unknown")

              println(
                s"$serviceId | $location | " +
                s"Occupied: $occupied | " +
                s"Capacity: $capacity | " +
                s"Available: $available"
              )
          }

        println("================================================")
      }
    }

    // ------------------------------------------------
    // WINDOW OPERATIONS
    // ------------------------------------------------

    val bookingWindow =
      bookingPairs
        .reduceByKeyAndWindow(
          (a: Int, b: Int) => a + b,
          Seconds(10),
          Seconds(5)
        )

    bookingWindow.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("================================================")
        println("          10-SECOND BOOKING WINDOW")
        println("================================================")

        rdd.collect()
          .sortBy(_._1)
          .foreach {
            case (serviceId, count) =>

              println(
                s"$serviceId -> $count bookings"
              )

              if (count >= 3) {
                println(
                  s"ALERT: High booking activity for $serviceId"
                )
              }
          }

        println("================================================")
      }
    }

    // ------------------------------------------------
    // SPARK SQL REPORTING
    // ------------------------------------------------

    persistedEvents.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        val bookingRows =
          rdd.map {
            case (
                  bookingId,
                  eventType,
                  serviceId,
                  customerId
                ) =>
              (
                bookingId,
                eventType,
                serviceId,
                customerId
              )
          }

        val bookingDF =
          bookingRows.toDF(
            "booking_id",
            "event_type",
            "service_id",
            "customer_id"
          )

        bookingDF.createOrReplaceTempView(
          "booking_events"
        )

        val report =
          spark.sql(
            """
              SELECT
                service_id,
                event_type,
                COUNT(*) AS event_count
              FROM booking_events
              GROUP BY service_id, event_type
              ORDER BY service_id, event_type
            """
          )

        println()
        println("================================================")
        println("             SPARK SQL REPORT")
        println("================================================")

        report.show(false)

        println("================================================")
      }
    }

    // ------------------------------------------------
    // START STREAMING
    // ------------------------------------------------

    ssc.start()

    println()
    println("================================================")
    println("       BOOKING STREAMING STARTED")
    println("================================================")
    println("Batch Interval : 5 seconds")
    println("Window Size    : 10 seconds")
    println("Slide Interval : 5 seconds")
    println("Socket         : localhost:9999")
    println()
    println("Input format:")
    println("bookingId,eventType,serviceId,customerId")
    println()
    println("Examples:")
    println("BK001,BOOK,HOTEL_H001,C001")
    println("BK002,BOOK,HOTEL_H001,C002")
    println("BK003,CANCEL,HOTEL_H001,C001")
    println("BK004,BOOK,FLIGHT_F001,C003")
    println("BK005,BOOK,BUS_B001,C004")
    println()
    println("Waiting for booking events...")
    println("================================================")
    println()

    ssc.awaitTermination()
  }
}
