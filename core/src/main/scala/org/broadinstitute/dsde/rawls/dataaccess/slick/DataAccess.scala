package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.expressions.SlickExpressionParser
import slick.dbio.Effect.Read
import slick.driver.JdbcProfile
import slick.jdbc.SQLActionBuilder
import slick.jdbc.SetParameter.SetUnit
import slick.jdbc.meta.MTable
import slick.profile.SqlAction

trait DataAccess
  extends PendingBucketDeletionComponent
  with RawlsUserComponent
  with RawlsGroupComponent
  with RawlsBillingProjectComponent
  with WorkspaceComponent
  with EntityComponent
  with AttributeComponent
  with MethodConfigurationComponent
  with SubmissionComponent
  with WorkflowComponent
  with ManagedGroupComponent
  with ExprEvalComponent
  with SlickExpressionParser {

  this: DriverComponent =>

  val driver: JdbcProfile
  val batchSize: Int
  
  import driver.api._

  def safeTableNames: DBIOAction[Seq[String], NoStream, Effect.Read] = {
    MTable.getTables.map { tables =>
      tables.map(_.name.name) flatMap {
        case "GROUP" => Option("`GROUP`")
        case "USER" => Option("`USER`")
        case "DATABASECHANGELOG" => None // managed by Liquibase
        case "DATABASECHANGELOGLOCK" => None // managed by Liquibase
        case other => Option(other)
      }
    }
  }

  def truncateAll: DBIOAction[Int, NoStream, Effect.Read with Effect] = {
    val truncates = safeTableNames flatMap { tableNames =>
      DBIO.fold(
        tableNames.map(tableName => SQLActionBuilder(List(s"TRUNCATE TABLE $tableName"), SetUnit).asUpdate), 0)(_ + _)
    }

    // need to temporarily disable foreign key checks for the truncate commands
    DBIO.sequence(
      Seq(
        sqlu"""SET FOREIGN_KEY_CHECKS = 0;""",
        truncates,
        sqlu"""SET FOREIGN_KEY_CHECKS = 1;"""
      )).map(results => results(1))  // return the results of the truncate queries
  }

  def sqlDBStatus() = {
    sql"select version()".as[String]
  }

}
