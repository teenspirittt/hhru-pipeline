# Project README

## Technologies Used
This project leverages several technologies to achieve its objectives:

1. **Python:** Used for writing Directed Acyclic Graphs (DAGs) to orchestrate the data pipeline.

2. **Airflow:** Utilized for creating and managing data pipelines, ensuring automation and scheduling of tasks.

3. **Hadoop HDFS:** Employed for storing and managing large datasets, where extracted job vacancies are stored.

4. **Scala Spark:** Utilized for processing and analyzing the data from Hadoop HDFS, providing a scalable and distributed data processing framework.

5. **Scala Akka:** Employed for implementing actors and asynchronous requests, enabling efficient, concurrent data processing.

## Project Structure
The project is organized into the following components and directories:

- **docker-hadoop:** A custom Docker image for running Hadoop. You can find it at [docker-hadoop on GitHub](https://github.com/teenspirittt/docker-hadoop).

- **docker-airflow:** A customized Docker image for running Apache Airflow. For detailed information, refer to [docker-airflow on GitHub](https://github.com/teenspirittt/docker-airflow).

- **src/dags/vacancies_extract_dag.py:** Python code for the Airflow DAG responsible for extracting job vacancies from the hh.ru API and storing them in Hadoop HDFS.

- **src/scala/CurrencyConverter.scala:** Scala code for a data processing task, possibly related to currency conversion.

- **src/scala/HRActivityAnalysis.scala:** Scala code for another data processing task, possibly related to HR activity analysis.

- **src/scala/build.sbt:** The SBT (Scala Build Tool) configuration file for the Scala project, defining dependencies and build settings.

- **src/scala/project/build.properties:** Configuration file for the Scala project's build settings.

- **src/scala/project/plugins.sbt:** Configuration file for SBT plugins used in the Scala project.

## How to Run the Project
To run this project, follow these steps:

1. In the project's root directory, start the Docker containers for Airflow and Hadoop:

   ```bash
   docker-compose up -f docker-compose.yaml -d # for Airflow
   docker-compose up -f docker-compose.yaml -d # for Hadoop



## Project Overview
The primary objective of this project is to utilize the hh.ru API to extract job vacancies and automate the data processing workflow. Here's a high-level overview of the project's process:

1. Job vacancies are retrieved from the hh.ru API.

2. The extracted data is stored in Hadoop HDFS for further processing.

3. Scala Spark is employed to process and analyze the data, which can include tasks like currency conversion and HR activity analysis.

4. A data mart is created, likely for reporting and data visualization purposes.

5. The data processing and visualization tasks are orchestrated and automated using Apache Airflow, ensuring a streamlined and scheduled workflow.

Please note that while this README provides an overview of the project, specific details about data processing, visualization, and PowerBI are not included. Refer to the project code and documentation for more information on those aspects.
