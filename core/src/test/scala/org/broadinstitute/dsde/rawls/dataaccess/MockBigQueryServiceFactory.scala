package org.broadinstitute.dsde.rawls.dataaccess

import java.util.UUID

import cats.effect._
import com.google.cloud.PageImpl
import com.google.cloud.bigquery.{BigQuery, Field, FieldValue, FieldValueList, JobId, LegacySQLTypeName, QueryJobConfiguration, Schema, TableResult}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

/*
 * Mocks for GoogleBigQueryServiceFactory and GoogleBigQueryService for use in unit tests.
 *
 * These mocks allow unit-test callers to specify the BigQuery result payload, and/or
 * specify that the query() method throws an exception.
 *
 * This file also contains the default fixture data returned by MockGoogleBigQueryService.query()
 * in the case where a caller did not override that result.
 */

object MockBigQueryServiceFactory {
  val F_INTEGER = Field.of("root.integer-field", LegacySQLTypeName.INTEGER)
  val F_BOOLEAN = Field.of("root.boolean-field", LegacySQLTypeName.BOOLEAN)
  val F_STRING = Field.of("root.datarepo_row_id", LegacySQLTypeName.STRING)
  val F_TIMESTAMP = Field.of("root.timestamp-field", LegacySQLTypeName.TIMESTAMP)

  val FV_INTEGER = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, "42")
  val FV_BOOLEAN = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, "true")
  val FV_TIMESTAMP = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, "1408452095.22")


  val table1Result = {
    // default fixture data returned by the underlying MockGoogleBigQueryService, unless a caller overrides it
    val schema: Schema = Schema.of(F_STRING, F_INTEGER, F_BOOLEAN, F_TIMESTAMP)

    val stringKeys = List("the first row", "the second row", "the third row")

    val results = stringKeys map { stringKey  =>
      FieldValueList.of(List(
        FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, stringKey),
        FV_INTEGER, FV_BOOLEAN, FV_TIMESTAMP).asJava,
        F_STRING, F_INTEGER, F_BOOLEAN, F_TIMESTAMP)
    }

    val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, results.asJava)
    val tableResult: TableResult = new TableResult(schema, 3, page)
    tableResult
  }

  val table2Result = {
    val table2RowCount = 123
    val schema: Schema = Schema.of(F_STRING, F_INTEGER, F_BOOLEAN, F_TIMESTAMP)

    val stringKeys = List.tabulate(table2RowCount)(i => "Row" + i)

    val results = stringKeys map { stringKey  =>
      FieldValueList.of(List(
        FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, stringKey),
        FV_INTEGER, FV_BOOLEAN, FV_TIMESTAMP).asJava,
        F_STRING, F_INTEGER, F_BOOLEAN, F_TIMESTAMP)
    }

    val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, results.asJava)
    val tableResult: TableResult = new TableResult(schema, table2RowCount, page)
    tableResult
  }

  def ioFactory(queryResponse: Either[Throwable, TableResult] = Right(table1Result)): MockBigQueryServiceFactory = {
    lazy val blocker = Blocker.liftExecutionContext(TestExecutionContext.testExecutionContext)
    implicit val ec = TestExecutionContext.testExecutionContext

    new MockBigQueryServiceFactory(blocker, queryResponse)
  }

}

class MockBigQueryServiceFactory(blocker: Blocker, queryResponse: Either[Throwable, TableResult])(implicit val executionContext: ExecutionContext)
  extends GoogleBigQueryServiceFactory(blocker: Blocker)(executionContext: ExecutionContext) {

  override def getServiceForPet(petKey: String): Resource[IO, GoogleBigQueryService[IO]] = {
    Resource.pure[IO, GoogleBigQueryService[IO]](new MockGoogleBigQueryService(queryResponse))
  }
}

class MockGoogleBigQueryService(queryResponse: Either[Throwable, TableResult]) extends GoogleBigQueryService[IO] {
  override def query(queryJobConfiguration: QueryJobConfiguration, options: BigQuery.JobOption*): IO[TableResult] =
    query(queryJobConfiguration, JobId.newBuilder().setJob(UUID.randomUUID().toString).build(), options: _*)

  override def query(queryJobConfiguration: QueryJobConfiguration, jobId: JobId, options: BigQuery.JobOption*): IO[TableResult] = {
    queryResponse match {
      case Left(t) => throw t
      case Right(results) => IO.pure(results)
    }
  }
}
