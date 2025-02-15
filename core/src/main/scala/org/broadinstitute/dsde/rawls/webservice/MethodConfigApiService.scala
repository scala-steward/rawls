package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.dsde.rawls.model.MethodRepoJsonSupport.AgoraEntityFormat
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.webservice.CustomDirectives._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext

/**
  * Created by dvoet on 6/4/15.
  */

trait MethodConfigApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  val workspaceServiceConstructor: UserInfo => WorkspaceService

  val methodConfigRoutes: server.Route = requireUserInfo() { userInfo =>
    path("workspaces" / Segment / Segment / "methodconfigs") { (workspaceNamespace, workspaceName) =>
      get {
        parameters( "allRepos".as[Boolean] ? false ) { allRepos =>
          if (allRepos) {
            complete { workspaceServiceConstructor(userInfo).listMethodConfigurations(WorkspaceName(workspaceNamespace, workspaceName)) }
          } else {
            complete { workspaceServiceConstructor(userInfo).listAgoraMethodConfigurations(WorkspaceName(workspaceNamespace, workspaceName)) }
          }
        }
      } ~
        post {
          entity(as[MethodConfiguration]) { methodConfiguration =>
            addLocationHeader(methodConfiguration.path(WorkspaceName(workspaceNamespace, workspaceName))) {
              complete {
                workspaceServiceConstructor(userInfo).createMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfiguration).map(StatusCodes.Created -> _)
              }
            }
          }
        }
    } ~
      path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment) { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).getMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName) }
        } ~
          put {
            entity(as[MethodConfiguration]) { newMethodConfiguration =>
              addLocationHeader(newMethodConfiguration.path(WorkspaceName(workspaceNamespace, workspaceName))) {
                complete {
                  workspaceServiceConstructor(userInfo).overwriteMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName, newMethodConfiguration)
                }
              }
            }
          } ~
          post {
            entity(as[MethodConfiguration]) { newMethodConfiguration =>
              complete { workspaceServiceConstructor(userInfo).updateMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName, newMethodConfiguration) }
            }
          } ~
          delete {
            complete { workspaceServiceConstructor(userInfo).deleteMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName).map(_ => StatusCodes.NoContent) }
          }
      } ~
      path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment / "validate") { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).getAndValidateMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName) }
        }
      } ~
      path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment / "rename") { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName) =>
        post {
          entity(as[MethodConfigurationName]) { newName =>
            complete { workspaceServiceConstructor(userInfo).renameMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigurationName, newName).map(_ => StatusCodes.NoContent) }
          }
        }
      } ~
      path("methodconfigs" / "copy") {
        post {
          entity(as[MethodConfigurationNamePair]) { confNames =>
            onSuccess(workspaceServiceConstructor(userInfo).copyMethodConfiguration(confNames)) { validatedMethodConfig =>
              addLocationHeader(validatedMethodConfig.methodConfiguration.path(confNames.destination.workspaceName)) {
                complete {
                  StatusCodes.Created -> validatedMethodConfig
                }
              }
            }
          }
        }
      } ~
      path("methodconfigs" / "copyFromMethodRepo") {
        post {
          entity(as[MethodRepoConfigurationImport]) { query =>
            onSuccess(workspaceServiceConstructor(userInfo).copyMethodConfigurationFromMethodRepo(query)) { validatedMethodConfig =>
              addLocationHeader(validatedMethodConfig.methodConfiguration.path(query.destination.workspaceName)) {
                complete {
                  StatusCodes.Created -> validatedMethodConfig
                }
              }
            }
          }
        }
      } ~
      path("methodconfigs" / "copyToMethodRepo") {
        post {
          entity(as[MethodRepoConfigurationExport]) { query =>
            complete { workspaceServiceConstructor(userInfo).copyMethodConfigurationToMethodRepo(query) }
          }
        }
      } ~
      path("methodconfigs" / "template") {
        post {
          entity(as[MethodRepoMethod]) { methodRepoMethod =>
            complete { workspaceServiceConstructor(userInfo).createMethodConfigurationTemplate(methodRepoMethod) }
          }
        }
      } ~
      path("methodconfigs" / "inputsOutputs") {
        post {
          entity(as[MethodRepoMethod]) { methodRepoMethod =>
            complete { workspaceServiceConstructor(userInfo).getMethodInputsOutputs(userInfo, methodRepoMethod) }
          }
        }
      }
  }
}