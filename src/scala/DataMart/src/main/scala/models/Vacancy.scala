package model

import java.sql.Timestamp

case class Vacancy(
  vacancyId: Long,
  vacancyName: String,
  employerName: String,
  regionName: String,
  averageSalary: Double,
  experience: Int,
  employment: String,
  publishDate: Timestamp,
  dayOfWeek: String,
  hour: String,
  day: String,
  month: String,
  year: String
)
