package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport._
import org.broadinstitute.dsde.rawls.model.{NamedDataRepoSnapshot, UserInfo, WorkspaceName}
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.snapshot.SnapshotService

import java.util.UUID
import scala.concurrent.ExecutionContext

trait SnapshotApiService extends UserInfoDirectives {

  implicit val executionContext: ExecutionContext

  val snapshotServiceConstructor: UserInfo => SnapshotService

  val snapshotRoutes: server.Route = requireUserInfo() { userInfo =>
    path("workspaces" /  Segment / Segment / "snapshots" / "v2") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[NamedDataRepoSnapshot]) { namedDataRepoSnapshot =>
          complete {
            snapshotServiceConstructor(userInfo).createSnapshot(WorkspaceName(workspaceNamespace, workspaceName), namedDataRepoSnapshot).map(StatusCodes.Created -> _)
          }
        }
      } ~
      get {
        // N.B. the "as[UUID]" delegates to SnapshotService.validateSnapshotId, which is in scope;
        // that method provides a 400 Bad Request response and nice error message
        parameters("offset".as[Int], "limit".as[Int], "referencedSnapshotId".as[UUID].optional) { (offset, limit, referencedSnapshotId) =>
          complete {
            snapshotServiceConstructor(userInfo).enumerateSnapshots(WorkspaceName(workspaceNamespace, workspaceName), offset, limit, referencedSnapshotId)
          }
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "snapshots" / "v2" / Segment) { (workspaceNamespace, workspaceName, snapshotId) =>
      get {
        complete {
          snapshotServiceConstructor(userInfo).getSnapshot(WorkspaceName(workspaceNamespace, workspaceName), snapshotId)
        }
      } ~
      patch {
        entity(as[UpdateDataRepoSnapshotReferenceRequestBody]) { updateDataRepoSnapshotReferenceRequestBody =>
          complete {
            snapshotServiceConstructor(userInfo).updateSnapshot(WorkspaceName(workspaceNamespace, workspaceName), snapshotId, updateDataRepoSnapshotReferenceRequestBody).map(_ => StatusCodes.NoContent)
          }
        }
      } ~
      delete {
        complete {
          snapshotServiceConstructor(userInfo).deleteSnapshot(WorkspaceName(workspaceNamespace, workspaceName), snapshotId).map(_ => StatusCodes.NoContent)
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "snapshots" / "v2" / "name" / Segment) { (workspaceNamespace, workspaceName, referenceName) =>
      get {
        complete {
          snapshotServiceConstructor(userInfo).getSnapshotByName(WorkspaceName(workspaceNamespace, workspaceName), referenceName)
        }
      }
    }
  }

}
