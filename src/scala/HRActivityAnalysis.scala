import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDate
import org.apache.spark.sql.expressions.UserDefinedFunction
import scala.concurrent.ExecutionContext.Implicits.global
import org.json4s._
import org.json4s.jackson.JsonMethods._

object HRActivityAnalysis {
  implicit val formats: DefaultFormats.type = DefaultFormats

  def main(args: Array[String]): Unit = {
    val date = LocalDate.now().toString
    val date_vacancies = s"$date" + "_vacancies"
    val date_cleaned = s"$date" + "_vacancies_cleaned"
    val raw_vacancies = "raw_vacancies"

    val spark = SparkSession.builder
      .appName("HRActivityAnalysis")
      .master("spark://sparkmaster:7077")
      .getOrCreate()

    val rawDF = spark.read.json(s"hdfs://namenode:9000/hadoop-data/$raw_vacancies.json")

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

    val salaryWithRublesDF = experienceDF.withColumn("average_salary",
      convertToRubles(col("salary.from"), col("salary.to"), col("salary.currency")))

    // Add ID, vacancy_name, and employment columns
    val enrichedDF = salaryWithRublesDF
      .withColumn("id", col("id").cast(LongType))
      .withColumn("vacancy_name", col("name"))
      .withColumn("employment", col("employment.name"))

    val selectedDF = enrichedDF.select(
      col("id").alias("vacancy_id"),
      col("vacancy_name"),
      col("employer.name").alias("employer_name"),
      col("area.name").alias("region_name"),
      col("average_salary"),
      col("experience_years").alias("experience"),
      col("employment"),
      col("published_at").alias("publish_date"),
      date_format(col("published_at"), "F").alias("day_of_week"),
      date_format(col("published_at"), "H").alias("hour"),
      date_format(col("published_at"), "d").alias("day"),
      date_format(col("published_at"), "M").alias("month"),
      date_format(col("published_at"), "y").alias("year")
    )

    val distinctDF = selectedDF.dropDuplicates()
    val cleanedDF = distinctDF.na.drop()

    cleanedDF.write.mode("overwrite").option("header", "true").csv(s"hdfs://namenode:9000/hadoop-data/$date_cleaned.csv")

    spark.stop()
  }
}
