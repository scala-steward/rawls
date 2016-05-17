package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestDriverComponent, TestDriverComponentWithFlatSpecAndMatchers}
import org.broadinstitute.dsde.rawls.jobexec.WorkflowSubmissionActor.{ScheduleNextWorkflowQuery, SubmitWorkflowBatch}
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses
import org.scalatest.{Matchers, FlatSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

/**
 * Created by dvoet on 5/17/16.
 */
class WorkflowSubmissionSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with TestDriverComponent {
  import driver.api._

  def this() = this(ActorSystem("WorkflowSubmissionSpec"))

  class TestWorkflowSubmission(
    val dataSource: SlickDataSource,
    val pollInterval: FiniteDuration = 1 second) extends WorkflowSubmission {

    val mockServer = RemoteServicesMockServer()

    val batchSize: Int = 3 // the mock remote server always returns 3, 2 success and an error
    val credential: Credential = new MockGoogleCredential.Builder().build()
    val googleServicesDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
    val executionServiceDAO = new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, mockServer.defaultWorkflowSubmissionTimeout)
    val methodRepoDAO = new HttpMethodRepoDAO(mockServer.mockServerBaseUrl)
  }

  "WorkflowSubmission" should "get a batch of workflows" in withDefaultTestDatabase {
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource)

    val workflowRecs = runAndWait(
      for {
        workflowRecs <- workflowQuery.findWorkflowsBySubmissionId(UUID.fromString(testData.submission1.submissionId)).take(workflowSubmission.batchSize).result
        _ <- workflowQuery.batchUpdateStatus(workflowRecs, WorkflowStatuses.Queued)
      } yield {
        workflowRecs
      }
    )

    assertResult(SubmitWorkflowBatch(workflowRecs.map(_.id))) {
      Await.result(workflowSubmission.getUnlaunchedWorkflowBatch(), Duration.Inf)
    }

    assert(runAndWait(workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).result).forall(_.status == WorkflowStatuses.Launching.toString))
  }

  it should "schedule the next check when there are no workflows" in withDefaultTestDatabase {
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource)
    assertResult(ScheduleNextWorkflowQuery) {
      Await.result(workflowSubmission.getUnlaunchedWorkflowBatch(), Duration.Inf)
    }
  }

  it should "have only 1 submission in a batch" in withDefaultTestDatabase {
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource)

    runAndWait(workflowQuery.batchUpdateStatus(WorkflowStatuses.Submitted, WorkflowStatuses.Queued))

    Await.result(workflowSubmission.getUnlaunchedWorkflowBatch(), Duration.Inf) match {
      case SubmitWorkflowBatch(workflowIds) =>
        assertResult(1)(runAndWait(workflowQuery.findWorkflowByIds(workflowIds).map(_.submissionId).distinct.result).size)
      case _ => fail("expected some workflows")
    }
  }
}
