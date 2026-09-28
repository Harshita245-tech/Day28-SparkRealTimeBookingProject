# 🚀 Day 28 - Real-Time Booking Streaming Project

## 📌 Project Overview

Day 28 implements a **Real-Time Booking Streaming System** using Apache Spark Structured components with DStreams, Pair RDDs, window operations, broadcast reference data, stateful processing, persistence, checkpointing, and Spark SQL reporting.

The project processes real-time booking and cancellation events for:

- 🏨 Hotels
- ✈️ Flights
- 🚌 Buses

The system maintains booking state, calculates occupancy and availability, detects high booking activity using windows, enriches events using broadcast reference data, and generates Spark SQL reporting output.

---

## 🎯 Objectives

- Process real-time hotel, flight, and bus booking events.
- Maintain booking and cancellation state.
- Calculate current occupancy.
- Calculate available capacity.
- Use Pair RDD operations.
- Apply window operations.
- Use broadcast reference data.
- Persist streaming data using `MEMORY_AND_DISK`.
- Use checkpointing for stateful processing.
- Generate Spark SQL reports.
- Understand real-time booking analytics.

---

## 🛠️ Technologies Used

- Apache Spark 3.5.3
- Scala 2.12.18
- Spark Streaming
- Spark SQL
- Pair RDD
- Window Operations
- Broadcast Variables
- Stateful Streaming
- Checkpointing
- Persistence
- SBT
- Netcat

---

## 📁 Project Structure

    day28-spark/
    ├── src/
    │   └── main/
    │       └── scala/
    │           └── Day28BookingStreaming.scala
    ├── project/
    │   └── build.properties
    ├── build.sbt
    ├── .gitignore
    └── README.md

---

## 📊 Booking Event Schema

The real-time input follows:

    bookingId,eventType,serviceId,customerId

### Example

    BK001,BOOK,HOTEL_H001,C001
    BK002,BOOK,HOTEL_H001,C002
    BK003,CANCEL,HOTEL_H001,C001
    BK004,BOOK,FLIGHT_F001,C003
    BK005,BOOK,BUS_B001,C004

### Fields

| Field | Description |
|---|---|
| bookingId | Unique booking identifier |
| eventType | BOOK or CANCEL |
| serviceId | Hotel, flight, or bus identifier |
| customerId | Customer identifier |

---

## 🏨 Reference Data

The project uses small reference datasets for service capacity and location.

### Capacity

| Service | Capacity |
|---|---:|
| HOTEL_H001 | 100 |
| FLIGHT_F001 | 180 |
| BUS_B001 | 50 |

### Location

| Service | Location |
|---|---|
| HOTEL_H001 | Hyderabad |
| FLIGHT_F001 | Delhi |
| BUS_B001 | Bangalore |

This reference information is distributed using Spark **Broadcast Variables**.

---

## 🔄 Real-Time Streaming

Spark Streaming reads booking events from:

    localhost:9999

The application uses a:

    5-second batch interval

Every incoming event is parsed and processed as part of a micro-batch.

---

## 🔑 Pair RDD Processing

Booking events are converted into key-value pairs.

Example:

    HOTEL_H001 -> 1
    HOTEL_H001 -> 1
    BUS_B001 -> 1

The project uses `reduceByKey` to calculate booking counts by service.

Example output:

    BUS_B001 -> 1 bookings

---

## ❌ Cancellation Processing

Cancellation events are separately identified and aggregated.

Example:

    HOTEL_H001 -> 1 cancellations

Cancellation events also affect the maintained occupancy state.

---

## 🔄 Booking and Cancellation State

The project maintains the current occupancy of each service.

A booking increases occupancy:

    BOOK -> +1

A cancellation decreases occupancy:

    CANCEL -> -1

The state is maintained using `updateStateByKey`.

This allows the application to retain information across multiple streaming batches.

---

## 📊 Occupancy and Availability

Availability is calculated using:

    Available = Capacity - Occupied

Example:

    HOTEL_H001 | Hyderabad | Occupied: 2 | Capacity: 100 | Available: 98

    FLIGHT_F001 | Delhi | Occupied: 1 | Capacity: 180 | Available: 179

    BUS_B001 | Bangalore | Occupied: 1 | Capacity: 50 | Available: 49

---

## 🪟 Window Operations

The project uses:

    Window Size    : 10 seconds
    Slide Interval : 5 seconds

`reduceByKeyAndWindow` calculates recent booking activity for each service.

Example:

    BUS_B001 -> 1 bookings

If a service receives at least 3 bookings within the window, a high booking activity alert is generated.

Example:

    ALERT: High booking activity for HOTEL_H001

---

## 📢 Broadcast Variables

Small reference datasets are broadcast to the Spark executors.

The project broadcasts:

- Service capacity
- Service location

Broadcasting avoids repeatedly transferring the same small reference data during processing.

---

## 💾 Persistence

Streaming events are persisted using:

    StorageLevel.MEMORY_AND_DISK

This allows Spark to keep reused streaming data in memory and spill to disk when required.

---

## 🛡️ Checkpointing

Checkpointing is configured for stateful streaming:

    /tmp/day28-booking-checkpoint

Checkpointing helps Spark recover state information and supports fault tolerance for stateful streaming operations.

---

## 🧮 Spark SQL Reporting

Each streaming batch is converted into a DataFrame.

The DataFrame is registered as:

    booking_events

Spark SQL is then used to generate service-wise event reports.

The reporting query groups events by:

- Service
- Event type
- Event count

Example output:

    +----------+----------+-----------+
    |service_id|event_type|event_count|
    +----------+----------+-----------+
    |BUS_B001  |BOOK      |1          |
    +----------+----------+-----------+

---

## 🧪 Testing

### Step 1 - Compile

    sbt compile

### Step 2 - Start Spark Streaming

    sbt run

### Step 3 - Open another terminal

    cd ~/day28-spark
    nc -lk 9999

### Step 4 - Send booking events

    BK001,BOOK,HOTEL_H001,C001
    BK002,BOOK,HOTEL_H001,C002
    BK003,BOOK,HOTEL_H001,C003
    BK004,CANCEL,HOTEL_H001,C001
    BK005,BOOK,FLIGHT_F001,C004
    BK006,BOOK,FLIGHT_F001,C005
    BK007,BOOK,BUS_B001,C006

---

## ✅ Observed Results

The streaming application successfully produced:

### Booking Events

    BOOKING EVENTS

### Booking Aggregation

    BOOKING COUNTS BY SERVICE

### Cancellation Aggregation

    CANCELLATION COUNTS

### Occupancy

    CURRENT OCCUPANCY

### Availability

    Available = Capacity - Occupied

### Window Processing

    10-SECOND BOOKING WINDOW

### Spark SQL

    SPARK SQL REPORT

---

## 📸 Sample Successful Output

    BUS_B001 | Bangalore | Occupied: 1 | Capacity: 50 | Available: 49

    FLIGHT_F001 | Delhi | Occupied: 1 | Capacity: 180 | Available: 179

    HOTEL_H001 | Hyderabad | Occupied: 2 | Capacity: 100 | Available: 98

---

## 🏗️ Processing Flow

    Booking Events
          |
          v
    Socket Stream
          |
          v
    Event Parsing
          |
          v
    Persistence
          |
          +----------------------+
          |                      |
          v                      v
    Pair RDD Aggregation    Stateful Processing
          |                      |
          v                      v
    Booking Counts          Occupancy
                                 |
                                 v
                          Availability
          |
          v
    Window Operations
          |
          v
    High Activity Detection
          |
          v
    Broadcast Reference Data
          |
          v
    Spark SQL Reporting

---

## 📚 Key Spark Concepts Demonstrated

- Spark Streaming
- DStreams
- Micro-batch processing
- Pair RDD
- `map`
- `filter`
- `reduceByKey`
- `reduceByKeyAndWindow`
- `updateStateByKey`
- Window operations
- Broadcast variables
- Persistence
- `MEMORY_AND_DISK`
- Checkpointing
- DataFrames
- Spark SQL
- Stateful streaming
- Real-time analytics

---

## 🌍 Real-World Scenario

This architecture can be applied to real-time booking platforms where:

- Hotels receive continuous reservations.
- Airlines receive flight bookings.
- Bus services receive ticket bookings.
- Cancellations change availability.
- High booking activity can be detected.
- Current occupancy can be monitored.
- Booking activity can be reported in real time.

---

## 🎓 Learning Outcome

This project demonstrates how Apache Spark can process real-time booking events and combine streaming, state management, Pair RDDs, window processing, broadcast data, persistence, checkpointing, and Spark SQL reporting into one end-to-end application.

---
---

## ⭐ Project Status

**Completed and tested successfully.**
