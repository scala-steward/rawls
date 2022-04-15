package org.broadinstitute.dsde.rawls.monitor

import akka.actor._
import akka.http.scaladsl.model.StatusCodes
import cats.Applicative
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.api.services.cloudbilling.model.ProjectBillingInfo
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.BillingAccountChange
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsBillingAccountName, RawlsBillingProject, RawlsBillingProjectName, Workspace}
import org.broadinstitute.dsde.rawls.monitor.WorkspaceBillingAccountMonitor.{CheckAll, UpdateBillingAccounts}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits.FutureToIO
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}

import java.sql.Timestamp
import java.time.Instant
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object WorkspaceBillingAccountMonitor {
  def props(datasource: SlickDataSource, gcsDAO: GoogleServicesDAO, initialDelay: FiniteDuration, pollInterval: FiniteDuration)(implicit executionContext: ExecutionContext): Props = {
    Props(new WorkspaceBillingAccountMonitor(datasource, gcsDAO, initialDelay, pollInterval))
  }

  sealed trait WorkspaceBillingAccountsMessage
  case object CheckAll extends WorkspaceBillingAccountsMessage
  case object UpdateBillingAccounts extends WorkspaceBillingAccountsMessage

  val BILLING_ACCOUNT_VALIDATION_ERROR_PREFIX = "Update Billing Account validation failed:"
}

class WorkspaceBillingAccountMonitor(dataSource: SlickDataSource, gcsDAO: GoogleServicesDAO, initialDelay: FiniteDuration, pollInterval: FiniteDuration)(implicit executionContext: ExecutionContext) extends Actor with LazyLogging {

  context.system.scheduler.scheduleWithFixedDelay(initialDelay, pollInterval, self, CheckAll)

  override def receive = {
    case CheckAll => checkAll()
    case UpdateBillingAccounts => updateBillingAccounts.unsafeRunSync()
  }

  def updateBillingAccounts: IO[Unit] =
    getBillingProjectChanges.flatMap(_.traverse_ { billingAccountChange =>
      for {
        billingProject <- loadBillingProject(billingAccountChange.billingProjectName)
        syncAttempt <- syncBillingProjectWithGoogle(billingProject).attempt
        _ <- recordBillingAccountSyncOutcome(billingAccountChange, Outcome.fromEither(syncAttempt))
        _ <- Applicative[IO].whenA(syncAttempt.isRight) {
          updateWorkspacesInBillingProject(billingProject)
        }
      } yield ()
    })


  private def getBillingProjectChanges: IO[List[BillingAccountChange]] =
    dataSource.inTransaction { _.billingAccountChangeQuery.getLatestChanges }.io.map(_.toList)


  private def loadBillingProject(projectName: RawlsBillingProjectName): IO[RawlsBillingProject] =
    dataSource
      .inTransaction(_.rawlsBillingProjectQuery.load(projectName))
      .io
      .map(_.getOrElse(throw new RawlsException(s"No such billing account $projectName")))


  private def syncBillingProjectWithGoogle(project: RawlsBillingProject): IO[Unit] =
    for {
      isV1BillingProject <- gcsDAO.rawlsCreatedGoogleProjectExists(project.googleProjectId).io
      _ <- Applicative[IO].whenA(isV1BillingProject)(
        updateBillingAccountOnGoogle(project.googleProjectId, project.billingAccount).io
      )
    } yield ()


  private def recordBillingAccountSyncOutcome(change: BillingAccountChange, outcome: Outcome): IO[Unit] =
    for {
      syncTime <- IO(Timestamp.from(Instant.now))
      (outcomeString, message) = Outcome.toTuple(outcome)
      _ <- dataSource.inTransaction { dataAccess =>
        import dataAccess.driver.api._

        dataAccess
          .billingAccountChangeQuery
          .filter(_.id === change.id)
          .map(c => (c.googleSyncTime, c.outcome, c.message))
          .update((syncTime.some, outcomeString.some, message)
        )
      }.io
    } yield ()


  private def updateWorkspacesInBillingProject(billingProject: RawlsBillingProject): IO[Unit] =
    listWorkspacesInProject(billingProject.projectName).flatMap(_.traverse_ { workspace =>
      for {
        syncAttempt <- if (workspace.googleProjectId != billingProject.googleProjectId)
          updateBillingAccountOnGoogle(workspace.googleProjectId, billingProject.billingAccount).io.attempt else
          IO.pure(Right())

        _ <- dataSource.inTransaction { dataAccess =>
          import dataAccess.driver.api._

          dataAccess
            .workspaceQuery
            .filter(_.id === workspace.workspaceIdAsUUID)
            .map(w => (w.currentBillingAccountOnGoogleProject, w.billingAccountErrorMessage))
            .update(billingProject.billingAccount.map(_.toString), syncAttempt match {
              case Right(_) => None
              case Left(throwable) => Option(throwable.getMessage)
            })
        }.io
      } yield ()
    })


  private def listWorkspacesInProject(billingProjectName: RawlsBillingProjectName): IO[List[Workspace]] =
    dataSource
      .inTransaction(_.workspaceQuery.listWithBillingProject(billingProjectName))
      .io
      .map(_.toList)


  private def checkAll() = {
    for {
      workspacesToUpdate <- dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listWorkspaceGoogleProjectsToUpdateWithNewBillingAccount()
      }
      _ = logger.info(s"Attempting to update workspaces: ${workspacesToUpdate.toList}")
      _ <- workspacesToUpdate.toList.traverse {
        case (googleProjectId, newBillingAccount, oldBillingAccount) =>
          IO.fromFuture(IO(updateBillingAccountInRawlsAndGoogle(googleProjectId, newBillingAccount, oldBillingAccount))).attempt.map {
            case Left(e) => {
              // We do not want to throw e here. traverse stops executing as soon as it encounters a Failure, but we
              // want to continue traversing the list to update the rest of the google project billing accounts even
              // if one of the update operations fails.
              logger.warn(s"Failed to update billing account from ${oldBillingAccount} to ${newBillingAccount} on project $googleProjectId", e)
              ()
            }
            case Right(res) => res
          }
      }.unsafeToFuture()
    } yield()
  }

  /**
    * Guiding Principle:  Always try to make Terra match what is on Google.  If they fall out of sync with each other,
    * Google wins.  If we don't know how to reconcile the difference, throw an error.
    * 1. DO NOT reenable billing if Google says Billing is disabled and Terra does not ALSO think billing is disabled
    * 2. DO NOT overwrite Billing Account if Terra and Google do not both agree on what the current Billing Account is
    *    2a. Except if Terra is trying to set the value to match whatever is currently set on Google
    * @param googleProjectId
    * @param newBillingAccount
    * @param oldBillingAccount
    * @param executionContext
    * @return
    */
  def updateBillingAccountInRawlsAndGoogle(googleProjectId: GoogleProjectId,
                                           newBillingAccount: Option[RawlsBillingAccountName],
                                           oldBillingAccount: Option[RawlsBillingAccountName])(implicit executionContext: ExecutionContext): Future[Unit] = {
    logger.info(s"Attempting to update Billing Account from ${oldBillingAccount} to ${newBillingAccount} on all Workspaces in Google Project ${googleProjectId}")
    for {
      _ <- updateBillingAccountOnGoogle(googleProjectId, newBillingAccount)
      _ <- setBillingAccountOnWorkspacesInProject(googleProjectId, newBillingAccount)
    } yield ()
  }

  private def updateBillingAccountOnGoogle(googleProjectId: GoogleProjectId, newBillingAccount: Option[RawlsBillingAccountName]): Future[Unit] = {
    val updateGoogleResult = for {
      projectBillingInfo <- gcsDAO.getBillingInfoForGoogleProject(googleProjectId)
      currentBillingAccountOnGoogle = getBillingAccountOption(projectBillingInfo)
      _ <- if (newBillingAccount != currentBillingAccountOnGoogle) {
        setBillingAccountOnGoogleProject(googleProjectId, newBillingAccount)
      } else {
        logger.info(s"Not updating Billing Account on Google Project ${googleProjectId} because currentBillingAccountOnGoogle:${currentBillingAccountOnGoogle} is the same as the newBillingAccount:${newBillingAccount}")
        Future.successful()
      }
    } yield ()

    updateGoogleResult.recoverWith {
      case e: RawlsExceptionWithErrorReport if e.errorReport.statusCode == Option(StatusCodes.Forbidden) && newBillingAccount.isDefined =>
        val message = s"Rawls does not have permission to set the Billing Account on ${googleProjectId} to ${newBillingAccount.get}"
        dataSource.inTransaction({ dataAccess =>
          dataAccess.rawlsBillingProjectQuery.updateBillingAccountValidity(newBillingAccount.get, isInvalid = true) >>
          dataAccess.workspaceQuery.updateWorkspaceBillingAccountErrorMessages(googleProjectId, s"${message} ${e.getMessage}")
        }).flatMap { _ =>
          logger.warn(message, e)
          Future.failed(e)
        }
      case e: Throwable =>
        dataSource.inTransaction { dataAccess =>
          dataAccess.workspaceQuery.updateWorkspaceBillingAccountErrorMessages(googleProjectId, e.getMessage)
        }.flatMap { _ =>
          Future.failed(e)
        }
    }
  }

  /**
    * Explicitly sets the Billing Account value on the given Google Project.  Any logic or conditionals controlling
    * whether this update gets called should be written in the calling method(s).
 *
    * @param googleProjectId
    * @param newBillingAccount
    */
  private def setBillingAccountOnGoogleProject(googleProjectId: GoogleProjectId,
                                               newBillingAccount: Option[RawlsBillingAccountName]): Future[ProjectBillingInfo] =
    newBillingAccount match {
      case Some(billingAccount) => gcsDAO.setBillingAccountName(googleProjectId, billingAccount)
      case None => gcsDAO.disableBillingOnGoogleProject(googleProjectId)
    }

  /**
    * Explicitly sets the Billing Account value for all Workspaces that are in the given Project.  Any logic or
    * conditionals controlling whether this update gets called should be written in the calling method(s).
    * @param googleProjectId
    * @param newBillingAccount
    * @return
    */
  private def setBillingAccountOnWorkspacesInProject(googleProjectId: GoogleProjectId,
                                                     newBillingAccount: Option[RawlsBillingAccountName]): Future[Int] =
    dataSource.inTransaction({ dataAccess =>
      dataAccess.workspaceQuery.updateWorkspaceBillingAccount(googleProjectId, newBillingAccount)
    })

  /**
    * Gets the Billing Account name out of a ProjectBillingInfo object wrapped in an Option[RawlsBillingAccountName] and
    * appropriately converts `null` or `empty` String into a None.
    * @param projectBillingInfo
    * @return
    */
  private def getBillingAccountOption(projectBillingInfo: ProjectBillingInfo): Option[RawlsBillingAccountName] = {
    if (projectBillingInfo.getBillingAccountName == null || projectBillingInfo.getBillingAccountName.isBlank)
      None
    else
      Option(RawlsBillingAccountName(projectBillingInfo.getBillingAccountName))
  }
}

final case class WorkspaceBillingAccountMonitorConfig(pollInterval: FiniteDuration, initialDelay: FiniteDuration)
