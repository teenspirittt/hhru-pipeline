import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDate
import org.apache.spark.sql.expressions.UserDefinedFunction
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import scala.concurrent.ExecutionContext
import akka.http.scaladsl.settings.ConnectionPoolSettings

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import org.json4s._
import org.json4s.jackson.JsonMethods._

object HRActivityAnalysis {
  implicit val formats: DefaultFormats.type = DefaultFormats
  implicit val system: ActorSystem = ActorSystem("my-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  val connectionPoolSettings = ConnectionPoolSettings(system).withMaxOpenRequests(5000)
  
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

    val salaryWithRublesDF = experienceDF.withColumn("average_salary",
      convertToRubles(col("salary.from"), col("salary.to"), col("salary.currency")))

    // Add ID, vacancy_name, and employment columns
    val enrichedDF = salaryWithRublesDF
      .withColumn("id", col("id").cast(LongType))
      .withColumn("vacancy_name", col("name"))
      .withColumn("employment", col("employment.name"))

    val vacancyIds: Seq[Long] = enrichedDF.select("id").rdd.map(_.getLong(0)).collect().toList

    val actorSystem = ActorSystem("RequestActors")
    val actorMaterializer = ActorMaterializer()

    // Здесь мы используем mapAsync вместо map, чтобы выполнять асинхронные запросы к API
    val futures: Seq[Future[(Long, List[String])]] = vacancyIds.map { vacancyId =>
      val jsonResponseFuture: Future[String] = fetchVacancyJson(vacancyId)
      jsonResponseFuture.map { jsonResponse =>
        val keySkills = extractKeySkills(jsonResponse)
        (vacancyId, keySkills)
      }(ExecutionContext.global)
    }

    val allSkills: Seq[(Long, List[String])] = Await.result(Future.sequence(futures), Duration.Inf)
    val skillsMap: Map[Long, List[String]] = allSkills.toMap

    actorSystem.terminate()

    val skillsDF = enrichedDF.withColumn("key_skills", map(lit(skillsMap)))

    val selectedDF = skillsDF.select(
      col("id").alias("vacancy_id"),
      col("vacancy_name"),
      col("employer.name").alias("employer_name"),
      col("area.name").alias("region_name"),
      col("average_salary"),
      col("experience_years").alias("experience"),
      col("key_skills").alias("skills"),
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

  def fetchVacancyJson(vacancyId: Long): Future[String] = {
    val url = s"https://api.hh.ru/vacancies/$vacancyId"
    val request = HttpRequest(uri = url)

    val responseFuture: Future[HttpResponse] = Http().singleRequest(request, settings = connectionPoolSettings)

    responseFuture.flatMap { response =>
      response.status match {
        case StatusCodes.OK =>
          Unmarshal(response.entity).to[String]
        case _ =>
          Future.failed(new RuntimeException(s"Request failed with status: ${response.status}"))
      }
    }
  }

  def extractKeySkills(jsonResponse: String): List[String] = {
    val json = org.json4s.native.JsonMethods.parse(jsonResponse)
    val keySkills = (json \ "key_skills").extractOpt[JValue] match {
      case Some(skillValue) =>
        skillValue match {
          case JString(skill) => List(skill)
          case JArray(skills) => skills.collect { case JString(skill) => skill }
          case _ => List()  // Если не удается преобразовать, вернуть пустой список
        }
      case None =>
        List()  // Если "key_skills" отсутствует, вернуть пустой список
    }
    keySkills
  }
}
