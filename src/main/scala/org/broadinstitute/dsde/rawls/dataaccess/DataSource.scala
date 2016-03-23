package org.broadinstitute.dsde.rawls.dataaccess

import java.util.concurrent.{Executors, ExecutorService}

import _root_.slick.backend.DatabaseConfig
import _root_.slick.driver.JdbcProfile
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadWriteAction, DataAccess, DataAccessComponent}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

import org.broadinstitute.dsde.rawls.util.ScalaConfig._

import liquibase.{Contexts, Liquibase}
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.{ClassLoaderResourceAccessor, ResourceAccessor}

object DataSource {
  def apply(databaseConfig: DatabaseConfig[JdbcProfile])(implicit executionContext: ExecutionContext): SlickDataSource = {
    new SlickDataSource(databaseConfig)
  }
}

class SlickDataSource(val databaseConfig: DatabaseConfig[JdbcProfile])(implicit executionContext: ExecutionContext) {
  private val database = databaseConfig.db

  /**
   * Create a special execution context, a fixed thread pool, to run each of our composite database actions. Running
   * each composite action as a runnable within the pool will ensure that-- at most-- the same number of actions are
   * running as there are available connections. Thus there should never be a connection deadlock, as outlined in
   * - https://github.com/slick/slick/issues/1274
   * - https://groups.google.com/d/msg/scalaquery/5MCUnwaJ7U0/NLLMotX9BQAJ
   *
   * Custom future thread pool based on:
   * - http://stackoverflow.com/questions/15285284/how-to-configure-a-fine-tuned-thread-pool-for-futures#comment23278672_15285441
   *
   * Database config parameter defaults based on: (expand the `forConfig` scaladoc for a full list of values)
   * - http://slick.typesafe.com/doc/3.1.0/api/index.html#slick.jdbc.JdbcBackend$DatabaseFactoryDef@forConfig(path:String,config:com.typesafe.config.Config,driver:java.sql.Driver,classLoader:ClassLoader):JdbcBackend.this.Database
   *
   * Reuses the error reporter from the database's executionContext.
   */
  private val actionThreadPool: ExecutorService = {
    val dbNumThreads = databaseConfig.config.getIntOr("db.numThreads", 20)
    val dbMaximumPoolSize = databaseConfig.config.getIntOr("db.maxConnections", dbNumThreads * 5)
    val actionThreadPoolSize = databaseConfig.config.getIntOr("actionThreadPoolSize", dbNumThreads) min dbMaximumPoolSize
    Executors.newFixedThreadPool(actionThreadPoolSize)
  }
  private val actionExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(
    actionThreadPool, database.executor.executionContext.reportFailure)

  val dataAccess = new DataAccessComponent(databaseConfig.driver)
  import dataAccess.driver.api._

  def inTransaction[T](f: (DataAccess) => ReadWriteAction[T]): Future[T] = {
    //database.run(f(dataAccess).transactionally) <-- https://github.com/slick/slick/issues/1274
    Future(Await.result(database.run(f(dataAccess).transactionally), Duration.Inf))(actionExecutionContext)
  }

  def initWithLiquibase(liquibaseChangeLog: String) = {
    val dbConnection = database.source.createConnection()
    val liquibaseConnection = new JdbcConnection(dbConnection)

    try {
      val resourceAccessor: ResourceAccessor = new ClassLoaderResourceAccessor()
      val liquibase = new Liquibase(liquibaseChangeLog, resourceAccessor, liquibaseConnection)
      liquibase.update(new Contexts())
    } finally {
      liquibaseConnection.close()
      dbConnection.close()
    }
  }

  // For testing/migration.  Not for production code!
  def initWithSlick(): Unit = {
    import dataAccess.driver.api._
    import scala.concurrent.duration._
    Await.result(database.run(DBIO.seq(dataAccess.allSchemas.create)), 1.minute)
  }
}
