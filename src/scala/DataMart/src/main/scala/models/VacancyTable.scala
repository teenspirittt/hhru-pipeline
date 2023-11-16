package model

import slick.jdbc.PostgresProfile.api._
import java.sql.Timestamp

class VacanciesTable(tag: Tag) extends Table[Vacancy](tag, "vacancies") {
  def vacancyId = column[Long]("vacancy_id", O.PrimaryKey)
  def vacancyName = column[String]("vacancy_name")
  def employerName = column[String]("employer_name")
  def regionName = column[String]("region_name")
  def averageSalary = column[Double]("average_salary")
  def experience = column[Int]("experience")
  def employment = column[String]("employment")
  def publishDate = column[Timestamp]("publish_date")
  def dayOfWeek = column[String]("day_of_week")
  def hour = column[String]("hour")
  def day = column[String]("day")
  def month = column[String]("month")
  def year = column[String]("year")

  def * = (
    vacancyId,
    vacancyName,
    employerName,
    regionName,
    averageSalary,
    experience,
    employment,
    publishDate,
    dayOfWeek,
    hour,
    day,
    month,
    year
  ).mapTo[Vacancy]
}
