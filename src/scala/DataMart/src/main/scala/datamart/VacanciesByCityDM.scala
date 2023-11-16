package datamarts

import database.DatabaseConnector.db
import slick.jdbc.PostgresProfile.api._
import scala.concurrent.ExecutionContext.Implicits.global

object VacanciesByCityDataMart {

  case class VacancyByCity(city: String, count: Int)

  class VacanciesByCity(tag: Tag) extends Table[VacancyByCity](tag, "vacancies_by_city") {
    def city: Rep[String] = column[String]("city", O.PrimaryKey)
    def count: Rep[Int] = column[Int]("count")

    def * : ProvenShape[VacancyByCity] = (city, count) <> (VacancyByCity.tupled, VacancyByCity.unapply)
  }

  val vacanciesByCityQuery: TableQuery[VacanciesByCity] = TableQuery[VacanciesByCity]

  def createTableIfNotExists: DBIOAction[Unit, NoStream, Effect.Schema] =
    vacanciesByCityQuery.schema.createIfNotExists

  def insertData(vacancies: Seq[VacancyByCity]): DBIOAction[Option[Int], NoStream, Effect.Write] =
    vacanciesByCityQuery ++= vacancies

  def getVacanciesByCity: DBIOAction[Seq[VacancyByCity], NoStream, Effect.Read] =
    vacanciesByCityQuery.result

  def main(args: Array[String]): Unit = {
    val createTableAction: DBIO[Unit] = createTableIfNotExists
    val insertDataAction: DBIO[Option[Int]] = insertData(Seq(VacancyByCity("City1", 20), VacancyByCity("City2", 30)))

    val setupFuture = db.run(createTableAction andThen insertDataAction)

    setupFuture.onComplete {
      case scala.util.Success(_) => println("DataMart 'VacanciesByCity' has been successfully created and populated.")
      case scala.util.Failure(ex) => println(s"Failed to setup DataMart 'VacanciesByCity': $ex")
    }
  }
}
