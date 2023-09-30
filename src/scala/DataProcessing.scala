import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.time.LocalDate

object DataProcessing {
  case class Vacancy(
      id: String,
      premium: Boolean,
      name: String,
      department: String,
      has_test: Boolean,
      response_letter_required: Boolean,
      area: String,
      salary_from: Option[Float],
      salary_to: Option[Float],
      currency: String,
      gross: Boolean,
      job_type: String,
      published_at: String,
      employer_id: String,
      employer_name: String,
      experience: String,
      employment: String,
      requirement: String,
      responsibility: String
  )

  def main(args: Array[String]): Unit = {
    val date = LocalDate.now().toString
    val date_vacancies = s"$date" + "_vacancies"
    val date_cleaned = s"$date" + "_vacancies_cleaned"

    val spark = SparkSession.builder
      .appName("DataProcessing")
      .master("spark://sparkmaster:7077")
      .getOrCreate()

    // Read raw JSON data into DataFrame
    val rawDF = spark.read.json(s"hdfs://namenode:9000/hadoop-data/$date_vacancies.json")

    // Select and transform relevant fields from raw data
    import spark.implicits._
    val vacanciesDF: DataFrame = rawDF.select(
      $"id",
      $"premium",
      $"name",
      $"department.id".alias("department"),
      $"has_test",
      $"response_letter_required",
      $"area.name".as("area"), // справочник
      $"salary.from".alias("salary_from"),
      $"salary.to".alias("salary_to"),
      $"salary.currency".alias("currency"),
      $"salary.gross",
      $"type.name".alias("job_type"),
      $"published_at",
      $"employer.id".alias("employer_id"),
      $"employer.name".alias("employer_name"),
      $"experience.name".alias("experience"),
      $"employment.name".alias("employment"),
      $"snippet.requirement".alias("requirement"),
      $"snippet.responsibility".alias("responsibility")
    )

    // Perform further data processing, analysis, and aggregation as needed
    val vacancyCountByArea = vacanciesDF.groupBy("area").count()

    val highestSalaryVacancies = vacanciesDF.orderBy($"salary_from".desc).limit(10)

    val avgSalaryByExperience = vacanciesDF.groupBy("experience")
      .agg(avg("salary_from").alias("average_salary"))c

    // Save the transformed data to Parquet format
    vacanciesDF.write.mode("overwrite").parquet(s"hdfs://namenode:9000/hadoop-data/$date_cleaned.parquet")

    spark.stop()
  }
}


// три датамарта,

// 1. как стать дата инженером... 
// 2. как стать стажером... 
// 3. работодатели по времени (по сезонам) spark on hot encoding
