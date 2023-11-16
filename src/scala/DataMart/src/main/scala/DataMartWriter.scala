package datamarts

import database.DatabaseConnector
import slick.jdbc.PostgresProfile.api._
import slick.lifted.TableQuery

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object DataMartWriter {

  // Пример для таблицы, замените на свои таблицы и классы
  case class CityVacancies(city: String, vacancyCount: Int)
  class CityVacanciesTable(tag: Tag) extends Table[CityVacancies](tag, "city_vacancies") {
    def city = column[String]("city", O.PrimaryKey)
    def vacancyCount = column[Int]("vacancy_count")

    def * = (city, vacancyCount) <> (CityVacancies.tupled, CityVacancies.unapply)
  }

  val cityVacanciesTable: TableQuery[CityVacanciesTable] = TableQuery[CityVacanciesTable]

  def writeCityVacancies(cityVacancies: Seq[CityVacancies]): Future[Unit] = {
    val db = DatabaseConnector.db

    val action = DBIO.seq(
      cityVacanciesTable.delete,
      cityVacanciesTable ++= cityVacancies
    )

    db.run(action).map(_ => ())
  }

  // Добавьте аналогичные методы для других DataMart'ов
}
