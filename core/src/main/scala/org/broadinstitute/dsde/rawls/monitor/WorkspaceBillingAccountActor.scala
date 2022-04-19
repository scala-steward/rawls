package org.broadinstitute.dsde.rawls.monitor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.Applicative
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.api.services.cloudbilling.model.ProjectBillingInfo
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.BillingAccountChange
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsBillingAccountName, RawlsBillingProject, RawlsBillingProjectName, Workspace}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits.FutureToIO
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome.{Failure, Success}

import java.sql.Timestamp
import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.{global => globalEc}
import scala.concurrent.duration._

object WorkspaceBillingAccountActor {
  sealed trait WorkspaceBillingAccountsMessage

  case object UpdateBillingAccounts extends WorkspaceBillingAccountsMessage

  def apply(dataSource: SlickDataSource,
            gcsDAO: GoogleServicesDAO,
            initialDelay: FiniteDuration,
            pollInterval: FiniteDuration): Behavior[WorkspaceBillingAccountsMessage] =
    Behaviors.setup { context =>
      val actor = WorkspaceBillingAccountActor(dataSource, gcsDAO)
      Behaviors.withTimers { scheduler =>
        scheduler.startTimerAtFixedRate(UpdateBillingAccounts, initialDelay, pollInterval)
        Behaviors.receiveMessage {
          case UpdateBillingAccounts =>
            try actor.updateBillingAccounts.unsafeRunSync()
            catch {
              case t: Throwable => context.executionContext.reportFailure(t)
            }
            Behaviors.same
        }
      }
    }
}

final case class WorkspaceBillingAccountActor(dataSource: SlickDataSource, gcsDAO: GoogleServicesDAO)
  extends LazyLogging {

  /* Sync billing account changes to billing projects and their associated workspaces with Google
   * one-at-a-time.
   *
   * Why not all-at-once?
   * To reduce the number of stale billing account changes we make.
   *
   * Let's imagine that there are lots of billing account changes to sync. If a user changes
   * the billing account on a billing project that's currently being sync'ed then we'll have to
   * sync the billing projects and all workspaces again immediately. If we sync one-at-a-time
   * we'll have a better chance of doing this once.
   */
  def updateBillingAccounts: IO[Unit] =
    getABillingProjectChange.flatMap(_.traverse_ { billingAccountChange =>
      for {
        billingProject <- loadBillingProject(billingAccountChange.billingProjectName)
        billingProjectSyncAttempt <- syncBillingProjectWithGoogle(billingProject).attempt
        _ <- recordBillingProjectSyncOutcome(billingAccountChange, Outcome.fromEither(billingProjectSyncAttempt))
        _ <- listWorkspacesInProject(billingProject.projectName).flatMap(_.traverse_ { workspace =>
          for {
            // error messages from syncing v1 billing projects are also written to the v1 workspace
            // record. We'll try to sync each v2 workspace regardless our of sheer bloody-mindedness.
            workspaceSyncAttempt <- if (workspace.googleProjectId != billingProject.googleProjectId)
              updateBillingAccountOnGoogle(workspace.googleProjectId, billingProject.billingAccount).attempt else
              IO.pure(billingProjectSyncAttempt)

            _ <- recordWorkspaceSyncOutcome(workspace, billingProject.billingAccount,
              Outcome.fromEither(workspaceSyncAttempt)
            )
          } yield ()
        })
      } yield ()
    })


  def getABillingProjectChange: IO[Option[BillingAccountChange]] =
    dataSource.inTransaction(_.billingAccountChangeQuery.getLatestChanges.map(_.headOption)).io


  private def loadBillingProject(projectName: RawlsBillingProjectName): IO[RawlsBillingProject] =
    dataSource
      .inTransaction(_.rawlsBillingProjectQuery.load(projectName))
      .io
      .map(_.getOrElse(throw new RawlsException(s"No such billing account $projectName")))


  private def syncBillingProjectWithGoogle(project: RawlsBillingProject): IO[Unit] =
    for {
      isV1BillingProject <- gcsDAO.rawlsCreatedGoogleProjectExists(project.googleProjectId).io
      _ <- Applicative[IO].whenA(isV1BillingProject)(
        updateBillingAccountOnGoogle(project.googleProjectId, project.billingAccount)
      )
    } yield ()


  private def recordBillingProjectSyncOutcome(change: BillingAccountChange, outcome: Outcome): IO[Unit] =
    for {
      syncTime <- IO(Timestamp.from(Instant.now))
      (outcomeString, message) = Outcome.toTuple(outcome)
      _ <- dataSource.inTransaction { dataAccess =>
        import dataAccess.driver.api._

        DBIO.seq(
          dataAccess
            .billingAccountChangeQuery
            .filter(_.id === change.id)
            .map(c => (c.googleSyncTime, c.outcome, c.message))
            .update((syncTime.some, outcomeString.some, message)),
          dataAccess
            .rawlsBillingProjectQuery
            .filter(_.projectName === change.billingProjectName.value)
            .map(p => (p.invalidBillingAccount, p.message))
            .update(outcome.isFailure, message)
        )
      }.io
    } yield ()


  private def listWorkspacesInProject(billingProjectName: RawlsBillingProjectName): IO[List[Workspace]] =
    dataSource
      .inTransaction(_.workspaceQuery.listWithBillingProject(billingProjectName))
      .io
      .map(_.toList)


  private def recordWorkspaceSyncOutcome(workspace: Workspace,
                                         billingAccount: Option[RawlsBillingAccountName],
                                         outcome: Outcome): IO[Unit] =
    dataSource.inTransaction { dataAccess =>
      import dataAccess.driver.api._

      val wsQuery = dataAccess
        .workspaceQuery
        .filter(_.id === workspace.workspaceIdAsUUID)

      DBIO.seq(
        if (outcome.isSuccess)
          wsQuery.map(_.currentBillingAccountOnGoogleProject).update(billingAccount.map(_.value)) else
          DBIO.successful(),
        wsQuery.map(_.billingAccountErrorMessage).update(outcome match {
          case Success => None
          case Failure(message) => Some(
            s"""Failed to set workspace Google Project "${workspace.googleProjectId}" Billing Account to "$billingAccount": "$message"."""
          )
        })
      )
    }.io.void


  private def updateBillingAccountOnGoogle(googleProjectId: GoogleProjectId, newBillingAccount: Option[RawlsBillingAccountName]): IO[Unit] =
    for {
      projectBillingInfo <- gcsDAO.getBillingInfoForGoogleProject(googleProjectId).io
      currentBillingAccountOnGoogle = getBillingAccountOption(projectBillingInfo)
      _ <- Applicative[IO].whenA(newBillingAccount != currentBillingAccountOnGoogle) {
        setBillingAccountOnGoogleProject(googleProjectId, newBillingAccount)
      }
    } yield ()

  /**
    * Explicitly sets the Billing Account value on the given Google Project.  Any logic or conditionals controlling
    * whether this update gets called should be written in the calling method(s).
    *
    * @param googleProjectId
    * @param newBillingAccount
    */
  private def setBillingAccountOnGoogleProject(googleProjectId: GoogleProjectId,
                                               newBillingAccount: Option[RawlsBillingAccountName]): IO[ProjectBillingInfo] =
    newBillingAccount match {
      case Some(billingAccount) => gcsDAO.setBillingAccountName(googleProjectId, billingAccount).io
      case None => gcsDAO.disableBillingOnGoogleProject(googleProjectId).io
    }


  /**
    * Gets the Billing Account name out of a ProjectBillingInfo object wrapped in an Option[RawlsBillingAccountName] and
    * appropriately converts `null` or `empty` String into a None.
    *
    * @param projectBillingInfo
    * @return
    */
  private def getBillingAccountOption(projectBillingInfo: ProjectBillingInfo): Option[RawlsBillingAccountName] =
    Option(projectBillingInfo.getBillingAccountName).filter(_.isBlank).map(RawlsBillingAccountName)

}

final case class WorkspaceBillingAccountMonitorConfig(pollInterval: FiniteDuration, initialDelay: FiniteDuration)
