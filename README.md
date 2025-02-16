This repository demonstrates the solution to processes and analyzes user interaction data from multiple platforms,ingests it into a NoSQL database with aggregations, and visualizes the results using a dashboard.

## Contents
- [Problem Breakdown](#problem-breakdown)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Use Case Design](#use-case-design)
- [Pipeline Execution](#pipeline-execution)
- [Acknowledgments](#acknowledgments)

<br>
<br>


## Problem Breakdown

#### Problem 1: Random Data Generator Kafka Producer
**Objective**: Create a random data generator that simulates user interaction logs and sends the generated data to a Kafka topic for streaming.

The data generated includes:
user_id: Unique identifier for each user.
item_id: Identifier for the item interacted with.
interaction_type: Type of interaction (click, view, purchase).
timestamp: Time of the interaction.
The generated data is produced to a Kafka topic, and the rate of data generation can be controlled for high-throughput simulations.
Implemented Code: interaction_events_kafka_producer.py

#### Problem 2: Kafka Consumer and Real Time Aggregations
**Objective**: Develop a Kafka consumer that consumes interaction data from the Kafka topic and performs real-time aggregations.

The Kafka consumer processes the data in real-time, calculating:
Average number of interactions per user.
Maximum and minimum interactions per item.
The processed data is ingested into a NoSQL database (MongoDB) to store aggregated results.
Implemented Code: kafka_mongodb_realtime_ingestion.py

#### Problem 3: Data Visualization and Dashboarding
Objective: Create a real-time dashboard that visualizes the aggregated data from the NoSQL database (MongoDB).

<br>

## Requirements

Before running the scripts, ensure you have the following dependencies installed:
- Python 3.7+
- Kafka: Kafka cluster running locally or in a cloud environment.
- MongoDB: MongoDB instance running locally or remotely for storing aggregated data
  
<br>

## Getting Started
#### 1. Random Data Generator Kafka Producer :
The script **interaction_events_kafka_producer.py** simulates user interaction data and publishes it to a Kafka topic.

**Usage**: 
- Modify the Kafka broker details if needed.
- Run the producer to start sending interaction data to Kafka.

#### 2. Kafka Consumer and Real Time Aggregations : 
The script **kafka_mongodb_realtime_ingestion.py** consumes data from the Kafka topic and performs real-time aggregations like average interactions per user and min/max interactions per item. This aggregated data is then stored in MongoDB.

**Usage**:
 - Ensuring Kafka is running and the topic is created.
 - Run the consumer to start processing and storing aggregated data in MongoDB.

#### 3. Data Visualization and Dashboarding
Visualization the aggregated data from MongoDB are done via MongoDB Charts.

Dashboard Link : [URL](https://charts.mongodb.com/charts-project-0-mfduida/embed/dashboards?id=2df046d3-40a8-42b4-89ac-d3f234b9aa20&theme=light&autoRefresh=true&maxDataAge=3600&showTitleAndDesc=false&scalingWidth=fixed&scalingHeight=fixed")

## Use Case Design

**Kafka**: Used as the message broker for real-time data streaming. Kafka provides high throughput and fault tolerance, ideal for handling large-scale real-time data.

**MongoDB**: Chosen as the NoSQL database due to its scalability, ease of integration, and ability to handle large volumes of data with flexible schema design.

**Python**: Python was used for its simplicity and rich ecosystem of libraries (e.g., pymongo, kafka-python, matplotlib).

## Pipeline Execution
The entire pipeline can be executed in sequence:

- Start the Kafka Producer to generate random interaction data and stream it to Kafka.
- Start the Kafka Consumer to process data, perform aggregations, and store results in MongoDB.
- Connection established between Dashboard & MongoDB Collection to visualize the results.

## Acknowledgments
- **Kafka:** Apache Kafka
- **MongoDB:** MongoDB
- **Python Libraries:** kafka-python, pymongo, matplotlib  


  
