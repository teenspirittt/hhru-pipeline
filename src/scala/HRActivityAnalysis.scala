import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDate
import org.apache.spark.sql.expressions.UserDefinedFunction


import CurrencyConverter._

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
    
    // work exp
    val experienceDF = rawDF.withColumn("experience_years", when(col("experience.name") === "Нет опыта", 0)
      .when(col("experience.name").contains("От") && col("experience.name").contains("до"), regexp_extract(col("experience.name"), "\\d+", 0))
      .when(col("experience.name").contains("Более"), regexp_extract(col("experience.name"), "\\d+", 0))
      .otherwise(null))


    // convert currency
    val convertToRubles: UserDefinedFunction = udf((from: Double, to: Double, currency: String) => {
      val rate = CurrencyConverter.getCurrencyRate(currency).getOrElse(1.0)
      val salaryRubles = if (from != 0 && to != 0) (from + to) / 2 * rate
                        else if (from != 0) from * rate
                        else if (to != 0) to * rate
                        else 0.0
      salaryRubles
    })
    
    // todo salary_average
    val salaryWithRublesDF = experienceDF.withColumn("average_salary",
      convertToRubles(col("salary.from"), col("salary.to"), col("salary.currency")))

  
    val selectedDF = experienceDF.select(
      col("published_at").alias("publish_date"),
      col("employer.name").alias("employer_name"),
      col("area.name").alias("region_name"),
      col("average_salary"),
      date_format(col("published_at"), "F").alias("day_of_week"),  // weekday (mon=1, sun=7)
      date_format(col("published_at"), "H").alias("hour"),         // hour (0-23)
      date_format(col("published_at"), "d").alias("day"),          // day (1-31)
      date_format(col("published_at"), "M").alias("month"),        // month (1-12)
      date_format(col("published_at"), "y").alias("year"),
      col("experience_years").alias("experience")                 // Добавляем опыт работы
    )

    val distinctDF = selectedDF.dropDuplicates()
    val cleanedDF = distinctDF.na.drop()

    selectedDF.write.mode("overwrite").csv(s"hdfs://namenode:9000/hadoop-data/$date_cleaned.csv")
    //rawDF.write.mode("overwrite").csv(s"hdfs://namenode:9000/hadoop-data/asd.csv")

    spark.stop()
  }
}
