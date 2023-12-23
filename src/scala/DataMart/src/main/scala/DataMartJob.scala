import datamart.AverageSalaryByCityProfession
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import java.time.LocalDate
import java.util.Properties

object DataMartJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("DataMartJob")
      .master("spark://sparkmaster:7077")
      .config("spark.executor.memory", "14g")
      .config("spark.driver.memory", "20g")
      .getOrCreate()

    val date = LocalDate.now().toString
    val data_cleaned = s"$date" + "_vacancies_cleaned"

    val dataSchema = StructType(Seq(
      StructField("vacancy_id", StringType, nullable = true),
      StructField("vacancy_name", StringType, nullable = true),
      StructField("employer_name", StringType, nullable = true),
      StructField("region_name", StringType, nullable = true),
      StructField("average_salary", DoubleType, nullable = true),
    ))

    val data = spark.read
      .schema(dataSchema)
      .format("csv")
      .option("header", "true")
      .load(s"hdfs://namenode:9000/hadoop-data/$data_cleaned.csv")
    import spark.implicits._

    val averageSalaryDF = data.groupBy("vacancy_name", "region_name")
      .agg(avg("average_salary").as("average_salary"))
      .as[AverageSalaryByCityProfession]

    averageSalaryDF.show()

    writeToPostgres(averageSalaryDF, "average_salary_by_city_profession")


    spark.stop()
  }

  private def writeToPostgres[T](ds: Dataset[T], tableName: String): Unit = {
    val jdbcUrl = "jdbc:postgresql://docker-airflow-postgres-1:5432/datamarts"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "airflow")
    connectionProperties.put("password", "airflow")
    connectionProperties.put("driver", "org.postgresql.Driver")

    ds.write.mode("overwrite")
      .jdbc(jdbcUrl, tableName, connectionProperties)
  }
}
