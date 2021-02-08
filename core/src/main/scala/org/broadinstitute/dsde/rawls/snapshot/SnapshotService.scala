package org.broadinstitute.dsde.rawls.snapshot

import java.util.UUID
import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.model.{CloningInstructionsEnum, DataReferenceDescription, DataReferenceList, DataRepoSnapshot, ReferenceTypeEnum}
import cats.effect.{ContextShift, IO}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{ErrorReport, GoogleProjectId, NamedDataRepoSnapshot, RawlsUserEmail, SamWorkspaceActions, UserInfo, WorkspaceAttributeSpecs, WorkspaceName}
import org.broadinstitute.dsde.rawls.util.{FutureSupport, WorkspaceSupport}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object SnapshotService {

  def constructor(dataSource: SlickDataSource, samDAO: SamDAO, workspaceManagerDAO: WorkspaceManagerDAO, bqServiceFactory: GoogleBigQueryServiceFactory, terraDataRepoUrl: String)(userInfo: UserInfo)
                 (implicit executionContext: ExecutionContext, contextShift: ContextShift[IO]): SnapshotService = {
    new SnapshotService(userInfo, dataSource, samDAO, workspaceManagerDAO, bqServiceFactory, terraDataRepoUrl)
  }

}

class SnapshotService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, val samDAO: SamDAO, workspaceManagerDAO: WorkspaceManagerDAO, bqServiceFactory: GoogleBigQueryServiceFactory, terraDataRepoInstanceName: String)
                     (implicit protected val executionContext: ExecutionContext, implicit val contextShift: ContextShift[IO])
  extends FutureSupport with WorkspaceSupport with LazyLogging {

  def CreateSnapshot(workspaceName: WorkspaceName, namedDataRepoSnapshot: NamedDataRepoSnapshot): Future[DataReferenceDescription] = createSnapshot(workspaceName, namedDataRepoSnapshot)
  def GetSnapshot(workspaceName: WorkspaceName, snapshotId: String): Future[DataReferenceDescription] = getSnapshot(workspaceName, snapshotId)
  def EnumerateSnapshots(workspaceName: WorkspaceName, offset: Int, limit: Int): Future[DataReferenceList] = enumerateSnapshots(workspaceName, offset, limit)
  def DeleteSnapshot(workspaceName: WorkspaceName, snapshotId: String): Future[Unit] = deleteSnapshot(workspaceName, snapshotId)

  def createSnapshot(workspaceName: WorkspaceName, snapshot: NamedDataRepoSnapshot): Future[DataReferenceDescription] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))).flatMap { workspaceContext =>
      if(!workspaceStubExists(workspaceContext.workspaceIdAsUUID, userInfo)) {
        workspaceManagerDAO.createWorkspace(workspaceContext.workspaceIdAsUUID, userInfo.accessToken)
      }

      val dataRepoReference = new DataRepoSnapshot().instanceName(terraDataRepoInstanceName).snapshot(snapshot.snapshotId)
      val ref = workspaceManagerDAO.createDataReference(workspaceContext.workspaceIdAsUUID, snapshot.name, ReferenceTypeEnum.DATA_REPO_SNAPSHOT, dataRepoReference, CloningInstructionsEnum.NOTHING, userInfo.accessToken)

      //create uncontrolled dataset resource in WSM here
      //create BQ dataset here

      for {
        petSAKey <- getPetSAKey(userInfo.userEmail, GoogleProjectId(workspaceName.namespace))
        createdDataset <- bqServiceFactory.getServiceForPet(petSAKey, GoogleProject(workspaceName.namespace)).use(_.createDataset("foo"))
      } yield {
        createdDataset
      }

      Future.successful(ref)
    }
  }

  private def getPetSAKey(userEmail: RawlsUserEmail, googleProject: GoogleProjectId): IO[String] = {
    logger.debug(s"getPetSAKey attempting against project ${googleProject.value}")
    IO.fromFuture(IO(
      samDAO.getPetServiceAccountKeyForUser(googleProject, userEmail)
        .recover {
          case report:RawlsExceptionWithErrorReport =>
            val errMessage = s"Error attempting to use project ${googleProject.value}. " +
              s"The project does not exist or you do not have permission to use it: ${report.errorReport.message}"
            throw new RawlsExceptionWithErrorReport(report.errorReport.copy(message = errMessage))
          case err:Exception => throw new RawlsException(s"Error attempting to use project ${googleProject.value}. " +
            s"The project does not exist or you do not have permission to use it: ${err.getMessage}")
        }))
  }

  def getSnapshot(workspaceName: WorkspaceName, snapshotId: String): Future[DataReferenceDescription] = {
    val snapshotUuid = validateSnapshotId(snapshotId)
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))).flatMap { workspaceContext =>
      val ref = workspaceManagerDAO.getDataReference(workspaceContext.workspaceIdAsUUID, snapshotUuid, userInfo.accessToken)
      Future.successful(ref)
    }
  }

  def enumerateSnapshots(workspaceName: WorkspaceName, offset: Int, limit: Int): Future[DataReferenceList] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))).map { workspaceContext =>
      Try(workspaceManagerDAO.enumerateDataReferences(workspaceContext.workspaceIdAsUUID, offset, limit, userInfo.accessToken)) match {
        case Success(references) => references
        // if we fail with a 404, it means we have no stub in WSM yet. This is benign and functionally equivalent
        // to having no references, so return the empty list.
        case Failure(ex: bio.terra.workspace.client.ApiException) if ex.getCode == 404 => new DataReferenceList()
        // but if we hit a different error, it's a valid error; rethrow it
        case Failure(ex: bio.terra.workspace.client.ApiException) =>
          throw new RawlsExceptionWithErrorReport(ErrorReport(ex.getCode, ex))
        case Failure(other) =>
          logger.warn(s"Unexpected error when enumerating snapshots: ${other.getMessage}")
          throw new RawlsExceptionWithErrorReport(ErrorReport(other))
      }
    }
  }

  def deleteSnapshot(workspaceName: WorkspaceName, snapshotId: String): Future[Unit] = {
    val snapshotUuid = validateSnapshotId(snapshotId)
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))).map { workspaceContext =>
      workspaceManagerDAO.deleteDataReference(workspaceContext.workspaceIdAsUUID, snapshotUuid, userInfo.accessToken)
    }
  }

  private def workspaceStubExists(workspaceId: UUID, userInfo: UserInfo): Boolean = {
    Try(workspaceManagerDAO.getWorkspace(workspaceId, userInfo.accessToken)).isSuccess
  }

  private def validateSnapshotId(snapshotId: String): UUID = {
    Try(UUID.fromString(snapshotId)) match {
      case Success(snapshotUuid) => snapshotUuid
      case Failure(_) =>
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "SnapshotId must be a valid UUID."))
    }
  }

}
