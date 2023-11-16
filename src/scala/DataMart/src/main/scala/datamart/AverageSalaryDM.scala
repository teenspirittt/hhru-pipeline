package datamarts

import database.DatabaseConnector.db
import slick.jdbc.PostgresProfile.api._
import scala.concurrent.ExecutionContext.Implicits.global

object AverageSalaryDataMart {

  case class AverageSalary(city: String, averageSalary: Double, minExperience: Int, maxExperience: Int)

  class AverageSalaryByCity(tag: Tag) extends Table[AverageSalary](tag, "average_salary_by_city") {
    def city: Rep[String] = column[String]("city", O.PrimaryKey)
    def averageSalary: Rep[Double] = column[Double]("average_salary")
    def minExperience: Rep[Int] = column[Int]("min_experience")
    def maxExperience: Rep[Int] = column[Int]("max_experience")

    def * : ProvenShape[AverageSalary] =
      (city, averageSalary, minExperience, maxExperience) <> (AverageSalary.tupled, AverageSalary.unapply)
  }

  val averageSalaryQuery: TableQuery[AverageSalaryByCity] = TableQuery[AverageSalaryByCity]

  def createTableIfNotExists: DBIOAction[Unit, NoStream, Effect.Schema] =
    averageSalaryQuery.schema.createIfNotExists

  def insertData(averageSalaries: Seq[AverageSalary]): DBIOAction[Option[Int], NoStream, Effect.Write] =
    averageSalaryQuery ++= averageSalaries

  def getAverageSalaryByCity: DBIOAction[Seq[AverageSalary], NoStream, Effect.Read] =
    averageSalaryQuery.result

  def main(args: Array[String]): Unit = {
    val createTableAction: DBIO[Unit] = createTableIfNotExists
    val insertDataAction: DBIO[Option[Int]] = insertData(
      Seq(AverageSalary("City1", 50000.0, 0, 2), AverageSalary("City2", 60000.0, 1, 5))
    )

    val setupFuture = db.run(createTableAction andThen insertDataAction)

    setupFuture.onComplete {
      case scala.util.Success(_) => println("DataMart 'AverageSalary' has been successfully created and populated.")
      case scala.util.Failure(ex) => println(s"Failed to setup DataMart 'AverageSalary': $ex")
    }
  }
}
