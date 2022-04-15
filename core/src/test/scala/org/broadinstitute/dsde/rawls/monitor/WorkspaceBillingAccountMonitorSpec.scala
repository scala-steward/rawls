package org.broadinstitute.dsde.rawls.monitor

import akka.http.scaladsl.model.StatusCodes
import cats.effect.unsafe.implicits.global
import com.google.api.services.cloudbilling.model.ProjectBillingInfo
import io.opencensus.trace.{Span => OpenCensusSpan}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadAction, TestDriverComponentWithFlatSpecAndMatchers}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.OptionValues
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

class WorkspaceBillingAccountMonitorSpec
  extends TestDriverComponentWithFlatSpecAndMatchers
    with MockitoSugar
    with OptionValues {

  val defaultExecutionContext: ExecutionContext = executionContext

  val defaultGoogleProjectNumber: GoogleProjectNumber = GoogleProjectNumber("42")
  val defaultBillingProjectName: RawlsBillingProjectName = RawlsBillingProjectName("test-bp")
  val defaultBillingAccountName: RawlsBillingAccountName = RawlsBillingAccountName("test-ba")


  "WorkspaceBillingAccountActor" should "update the billing account on all v1 and v2 workspaces in a billing project" in 
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingAccountName = defaultBillingAccountName
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, Option(billingAccountName), None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val v1Workspace = Workspace(billingProject.projectName.value, "v1", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId(billingProject.projectName.value), billingProject.googleProjectNumber, billingProject.billingAccount, None, Option(DateTime.now), WorkspaceType.RawlsWorkspace)
      val v2Workspace = Workspace(billingProject.projectName.value, "v2", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("differentId"), Option(GoogleProjectNumber("43")), billingProject.billingAccount, None, Option(DateTime.now), WorkspaceType.RawlsWorkspace)
      val workspaceWithoutBillingAccount = v2Workspace.copy(
        name = UUID.randomUUID().toString,
        currentBillingAccountOnGoogleProject = None
      )

      val newBillingAccount = RawlsBillingAccountName("new-ba")

      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(billingProject)
          _ <- workspaceQuery.createOrUpdate(v1Workspace)
          _ <- workspaceQuery.createOrUpdate(v2Workspace)
          _ <- workspaceQuery.createOrUpdate(workspaceWithoutBillingAccount)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(newBillingAccount), testData.userOwner.userSubjectId)
        } yield ()
      }

      WorkspaceBillingAccountActor(dataSource, gcsDAO = new MockGoogleServicesDAO("test"))
        .updateBillingAccounts
        .unsafeRunSync

      every (
        runAndWait(workspaceQuery.listWithBillingProject(billingProject.projectName))
          .map(_.currentBillingAccountOnGoogleProject)
      ) shouldBe Some(newBillingAccount)
    }
  

  it should "not endlessly retry when it fails to get billing info for the google project" in 
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(defaultBillingAccountName)
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, originalBillingAccount, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val workspace = Workspace(billingProject.projectName.value, "whatever", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("any old project"), Option(GoogleProjectNumber("44")), originalBillingAccount, None, Option(DateTime.now), WorkspaceType.RawlsWorkspace)

      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(billingProject)
          _ <- workspaceQuery.createOrUpdate(workspace)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(RawlsBillingAccountName("new-ba")), testData.userOwner.userSubjectId)
        } yield ()
      }

      val exceptionMessage = "oh what a shame!  It went kerplooey!"
      val failingGcsDao = spy(new MockGoogleServicesDAO("") {
        override def getBillingInfoForGoogleProject(googleProjectId: GoogleProjectId)(implicit executionContext: ExecutionContext): Future[ProjectBillingInfo] =
          Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(exceptionMessage)))
      })

      WorkspaceBillingAccountActor(dataSource, gcsDAO = failingGcsDao)
        .updateBillingAccounts
        .unsafeRunSync

      runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName))
        .getOrElse(fail("workspace not found"))
        .billingAccountErrorMessage.value should include(exceptionMessage)

      verify(failingGcsDao, times(1)).getBillingInfoForGoogleProject(workspace.googleProjectId)
    }
  

  it should "not endlessly retry when it fails to set billing info for the google project" in 
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(defaultBillingAccountName)
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, originalBillingAccount, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val workspace = Workspace(billingProject.projectName.value, "whatever", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("any old project"), Option(GoogleProjectNumber("44")), originalBillingAccount, None, Option(DateTime.now), WorkspaceType.RawlsWorkspace)

      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(billingProject)
          _ <- workspaceQuery.createOrUpdate(workspace)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(RawlsBillingAccountName("new-ba")), testData.userOwner.userSubjectId)
        } yield ()
      }

      val exceptionMessage = "oh what a shame!  It went kerplooey!"
      val failingGcsDao = spy(new MockGoogleServicesDAO("") {
        override def setBillingAccountName(googleProjectId: GoogleProjectId, billingAccountName: RawlsBillingAccountName, span: OpenCensusSpan = null): Future[ProjectBillingInfo] =
          Future.failed(new RawlsException(exceptionMessage))
      })

      WorkspaceBillingAccountActor(dataSource, gcsDAO = failingGcsDao)
        .updateBillingAccounts
        .unsafeRunSync

      runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName))
        .getOrElse(fail("workspace not found"))
        .billingAccountErrorMessage.value should include(exceptionMessage)

      verify(failingGcsDao, times(1)).getBillingInfoForGoogleProject(workspace.googleProjectId)
    }
  

  it should "not try to update the billing account if the new value is the same as the old value" in 
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccountName = defaultBillingAccountName
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, Option(originalBillingAccountName), None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val workspace = Workspace(billingProject.projectName.value, "whatever", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("any old project"), Option(GoogleProjectNumber("44")), Option(originalBillingAccountName), None, Option(DateTime.now), WorkspaceType.RawlsWorkspace)

      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(billingProject)
          _ <- workspaceQuery.createOrUpdate(workspace)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(originalBillingAccountName), testData.userOwner.userSubjectId)
        } yield ()
      }

      val mockGcsDAO = spy(new MockGoogleServicesDAO("") {
        override def getBillingInfoForGoogleProject(googleProjectId: GoogleProjectId)(implicit executionContext: ExecutionContext): Future[ProjectBillingInfo] =
          Future.successful(new ProjectBillingInfo().setBillingAccountName(originalBillingAccountName.value).setBillingEnabled(true))
      })

      WorkspaceBillingAccountActor(dataSource, gcsDAO = mockGcsDAO)
        .updateBillingAccounts
        .unsafeRunSync

      runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName))
        .getOrElse(fail("workspace not found"))
        .currentBillingAccountOnGoogleProject.value shouldBe originalBillingAccountName

      verify(mockGcsDAO, times(0)).setBillingAccountName(workspace.googleProjectId, originalBillingAccountName)
    }
  

  it should "continue to update other workspace google projects even if one fails to update" in 
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(defaultBillingAccountName)
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, originalBillingAccount, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val workspace1 = Workspace(billingProject.projectName.value, "workspace1", UUID.randomUUID().toString, "bucketName1", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId(billingProject.projectName.value), billingProject.googleProjectNumber, originalBillingAccount, None, Option(DateTime.now), WorkspaceType.RawlsWorkspace)
      val workspace2 = Workspace(billingProject.projectName.value, "workspace2", UUID.randomUUID().toString, "bucketName2", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId("differentId"), Option(GoogleProjectNumber("43")), originalBillingAccount, None, Option(DateTime.now), WorkspaceType.RawlsWorkspace)
      val badWorkspaceGoogleProjectId = GoogleProjectId("very bad")
      val badWorkspace = Workspace(billingProject.projectName.value, "bad", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, badWorkspaceGoogleProjectId, Option(GoogleProjectNumber("44")), originalBillingAccount, None, Option(DateTime.now), WorkspaceType.RawlsWorkspace)

      val newBillingAccount = RawlsBillingAccountName("new-ba")

      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(billingProject)
          _ <- workspaceQuery.createOrUpdate(workspace1)
          _ <- workspaceQuery.createOrUpdate(workspace2)
          _ <- workspaceQuery.createOrUpdate(badWorkspace)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(newBillingAccount), testData.userOwner.userSubjectId)
        } yield ()
      }

      val exceptionMessage = "oh what a shame!  It went kerplooey!"
      val failingGcsDao = new MockGoogleServicesDAO("") {
        override def getBillingInfoForGoogleProject(googleProjectId: GoogleProjectId)(implicit executionContext: ExecutionContext): Future[ProjectBillingInfo] = {
          if (googleProjectId == badWorkspaceGoogleProjectId) {
            Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, exceptionMessage)))
          } else {
            super.getBillingInfoForGoogleProject(googleProjectId)
          }
        }
      }

      WorkspaceBillingAccountActor(dataSource, gcsDAO = failingGcsDao)
        .updateBillingAccounts
        .unsafeRunSync

      def getBillingAccountOnGoogleProject(workspace: Workspace): ReadAction[(Option[String], Option[String])] =
        workspaceQuery
          .findByIdOrFail(workspace.workspaceId)
          .map(ws => (ws.currentBillingAccountOnGoogleProject.map(_.toString), ws.billingAccountErrorMessage))
          
      runAndWait {
        for {
          (ws1BillingAccountOnGoogleProject, _) <- getBillingAccountOnGoogleProject(workspace1)
          (ws2BillingAccountOnGoogleProject, _) <- getBillingAccountOnGoogleProject(workspace2)
          (_, badWsBillingAccountErrorMessage) <- getBillingAccountOnGoogleProject(badWorkspace)
        } yield {
          ws1BillingAccountOnGoogleProject shouldBe Some(newBillingAccount)
          ws2BillingAccountOnGoogleProject shouldBe Some(newBillingAccount)
          badWsBillingAccountErrorMessage.value should include(exceptionMessage)
        }
      }
    }
  

  // TODO: CA-1235 Remove during cleanup once all workspaces have their own Google project
  it should "propagate error messages to all workspaces in a Google project" in 
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(RawlsBillingAccountName("original-ba"))
      val billingProject = RawlsBillingProject(RawlsBillingProjectName("v1-Billing-Project"), CreationStatuses.Ready, originalBillingAccount, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val v1GoogleProjectId = GoogleProjectId(billingProject.projectName.value)
      val firstV1Workspace  = Workspace(billingProject.projectName.value, "first-v1-workspace",  UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, v1GoogleProjectId, billingProject.googleProjectNumber, originalBillingAccount, None, Option(DateTime.now), WorkspaceType.RawlsWorkspace)
      val secondV1Workspace = Workspace(billingProject.projectName.value, "second-v1-workspace", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, v1GoogleProjectId, billingProject.googleProjectNumber, originalBillingAccount, None, Option(DateTime.now), WorkspaceType.RawlsWorkspace)
      val v2Workspace = Workspace(billingProject.projectName.value, "v2 workspace", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("v2WorkspaceGoogleProject"), Option(GoogleProjectNumber("43")), originalBillingAccount, None, Option(DateTime.now), WorkspaceType.RawlsWorkspace)

      val newBillingAccount = RawlsBillingAccountName("new-ba")

      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(billingProject)
          _ <- workspaceQuery.createOrUpdate(firstV1Workspace)
          _ <- workspaceQuery.createOrUpdate(v2Workspace)
          _ <- workspaceQuery.createOrUpdate(secondV1Workspace)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(newBillingAccount), testData.userOwner.userSubjectId)
        } yield ()
      }

      val exceptionMessage = "oh what a shame!  It went kerplooey!"
      val failingGcsDao = spy(new MockGoogleServicesDAO("") {
        override def setBillingAccountName(googleProjectId: GoogleProjectId, billingAccountName: RawlsBillingAccountName, span: OpenCensusSpan = null): Future[ProjectBillingInfo] = {
          if (googleProjectId == v1GoogleProjectId) {
            Future.failed(new RawlsException(exceptionMessage))
          } else {
            super.getBillingInfoForGoogleProject(googleProjectId)
          }
        }
      })

      WorkspaceBillingAccountActor(dataSource, gcsDAO = failingGcsDao)
        .updateBillingAccounts
        .unsafeRunSync

      def getBillingAccountErrorMessage(workspace: Workspace): ReadAction[Option[String]] =
        workspaceQuery.findByIdOrFail(workspace.workspaceId).map(_.billingAccountErrorMessage)

      runAndWait {
        for {
          firstV1WsError <- getBillingAccountErrorMessage(firstV1Workspace)
          secondV1WsError <- getBillingAccountErrorMessage(secondV1Workspace)
          v2WsError <- getBillingAccountErrorMessage(v2Workspace)
        } yield  {
          firstV1WsError.value should include(exceptionMessage)
          secondV1WsError.value should include(exceptionMessage)
          v2WsError shouldBe empty
        }
      }

      verify(failingGcsDao, times(1)).setBillingAccountName(
        ArgumentMatchers.eq(v1GoogleProjectId), ArgumentMatchers.eq(newBillingAccount),
        any[OpenCensusSpan]
      )
    }

  it should "mark a billing project's billing account as invalid if Google returns a 403" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(defaultBillingAccountName)
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, originalBillingAccount, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val v1Workspace = Workspace(billingProject.projectName.value, "v1", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId(billingProject.projectName.value), billingProject.googleProjectNumber, originalBillingAccount, None, Option(DateTime.now), WorkspaceType.RawlsWorkspace)
      val v2Workspace = Workspace(billingProject.projectName.value, "v2", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("differentId"), Option(GoogleProjectNumber("43")), originalBillingAccount, None, Option(DateTime.now), WorkspaceType.RawlsWorkspace)
      val newBillingAccount = RawlsBillingAccountName("new-ba")

      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(billingProject)
          project <- rawlsBillingProjectQuery.load(billingProject.projectName)
          _ <- workspaceQuery.createOrUpdate(v1Workspace)
          _ <- workspaceQuery.createOrUpdate(v2Workspace)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(newBillingAccount), testData.userOwner.userSubjectId)
        } yield project.value.invalidBillingAccount shouldBe false
      }

      val exceptionMessage = "Naughty naughty!  You ain't got no permissions!"
      val failingGcsDao = new MockGoogleServicesDAO("") {
        override def setBillingAccountName(googleProjectId: GoogleProjectId, billingAccountName: RawlsBillingAccountName, span: OpenCensusSpan = null): Future[ProjectBillingInfo] = {
          Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, exceptionMessage)))
        }
      }

      WorkspaceBillingAccountActor(dataSource, gcsDAO = failingGcsDao)
        .updateBillingAccounts
        .unsafeRunSync

      runAndWait {
        for {
          project <- rawlsBillingProjectQuery.load(billingProject.projectName)
          workspaces <- workspaceQuery.listWithBillingProject(billingProject.projectName)
        } yield {
          project.value.invalidBillingAccount shouldBe true
          every (workspaces.map(_.billingAccountErrorMessage.value)) should include(exceptionMessage)
        }
      }
    }

}
