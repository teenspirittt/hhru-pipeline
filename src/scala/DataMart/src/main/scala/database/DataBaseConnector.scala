package database

import slick.jdbc.PostgresProfile.api._

object DatabaseConnector {

  val db: Database = Database.forConfig("postgres")
}