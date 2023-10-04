import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDate

object HRActivityAnalysis {
  def main(args: Array[String]): Unit = {
    val date = LocalDate.now().toString
    val date_vacancies = s"$date" + "_vacancies"
    val date_cleaned = s"$date" + "_vacancies_cleaned"

    val spark = SparkSession.builder
      .appName("HRActivityAnalysis")
      .master("spark://sparkmaster:7077")
      .getOrCreate()

    val rawDF = spark.read.json(s"hdfs://namenode:9000/hadoop-data/$date_vacancies.json")
    

    val salaryDF = rawDF.withColumn("average_salary", when(col("salary.from").isNotNull && col("salary.to").isNotNull,
      (col("salary.from") + col("salary.to")) / 2)
      .otherwise(when(col("salary.from").isNotNull, col("salary.from"))
      .otherwise(when(col("salary.to").isNotNull, col("salary.to"))
      .otherwise(lit(null)))))


    val filteredDF = salaryDF.na.drop()

    val selectedDF = filteredDF.select(
      col("published_at").alias("publish_date"),
      col("employer.name").alias("employer_name"),
      col("area.name").alias("region_name"),
      col("average_salary"),
      date_format(col("published_at"), "u").alias("day_of_week"),  // weekday (mon=1, sun=7)
      date_format(col("published_at"), "H").alias("hour"),         // hour (0-23)
      date_format(col("published_at"), "d").alias("day"),          // day (1-31)
      date_format(col("published_at"), "M").alias("month"),        // month (1-12)
      date_format(col("published_at"), "y").alias("year")         
    )

    selectedDF.write.parquet(s"hdfs://namenode:9000/hadoop-data/$date_cleaned.parquet")

    spark.stop()
  }
}
