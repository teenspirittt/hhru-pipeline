package datamarts

import database.DatabaseConnector.db
import slick.jdbc.PostgresProfile.api._
import scala.concurrent.ExecutionContext.Implicits.global

object TechnologyDemandDataMart {

  case class TechnologyDemand(city: String, frontendCount: Int, backendCount: Int, cLanguageCount: Int)

  class TechnologyDemandByCity(tag: Tag) extends Table[TechnologyDemand](tag, "technology_demand_by_city") {
    def city: Rep[String] = column[String]("city", O.PrimaryKey)
    def frontendCount: Rep[Int] = column[Int]("frontend_count")
    def backendCount: Rep[Int] = column[Int]("backend_count")
    def cLanguageCount: Rep[Int] = column[Int]("c_language_count")

    def * : ProvenShape[TechnologyDemand] =
      (city, frontendCount, backendCount, cLanguageCount) <> (TechnologyDemand.tupled, TechnologyDemand.unapply)
  }

  val technologyDemandQuery: TableQuery[TechnologyDemandByCity] = TableQuery[TechnologyDemandByCity]

  def createTableIfNotExists: DBIOAction[Unit, NoStream, Effect.Schema] =
    technologyDemandQuery.schema.createIfNotExists

  def insertData(demand: TechnologyDemand): DBIOAction[Option[Int], NoStream, Effect.Write] =
    technologyDemandQuery += demand

  def getTechnologyDemandByCity: DBIOAction[Seq[TechnologyDemand], NoStream, Effect.Read] =
    technologyDemandQuery.result

  def main(args: Array[String]): Unit = {
    val createTableAction: DBIO[Unit] = createTableIfNotExists
    val insertDataAction: DBIO[Option[Int]] = insertData(
      TechnologyDemand("City1", 10, 15, 5)
    )

    val setupFuture = db.run(createTableAction andThen insertDataAction)

    setupFuture.onComplete {
      case scala.util.Success(_) => println("DataMart 'TechnologyDemand' has been successfully created and populated.")
      case scala.util.Failure(ex) => println(s"Failed to setup DataMart 'TechnologyDemand': $ex")
    }
  }
}
