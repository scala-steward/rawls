package org.broadinstitute.dsde.workbench.sam.google

import akka.actor.SupervisorStrategy.{Escalate, Stop}
import akka.actor._
import akka.pattern._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.PubSubMessage
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, RawlsBillingProjectName}
import org.broadinstitute.dsde.workbench.util.FutureSupport
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

/**
  * Created by dvoet on 12/6/16.
  */
object BillingProjectMonitorSupervisor {
  sealed trait BillingProjectMonitorSupervisorMessage
  case object Init extends BillingProjectMonitorSupervisorMessage
  case object Start extends BillingProjectMonitorSupervisorMessage

  def props(
             pollInterval: FiniteDuration,
             pollIntervalJitter: FiniteDuration,
             pubSubDao: GooglePubSubDAO,
             pubSubTopicName: String,
             pubSubSubscriptionName: String,
             workerCount: Int,
             datasource: SlickDataSource): Props =
    Props(
      new BillingProjectMonitorSupervisor(pollInterval, pollIntervalJitter, pubSubDao, pubSubTopicName, pubSubSubscriptionName, workerCount, datasource))
}

class BillingProjectMonitorSupervisor(
                                        val pollInterval: FiniteDuration,
                                        pollIntervalJitter: FiniteDuration,
                                        pubSubDao: GooglePubSubDAO,
                                        pubSubTopicName: String,
                                        pubSubSubscriptionName: String,
                                        workerCount: Int,
                                        datasource: SlickDataSource)
  extends Actor
    with LazyLogging {
  import BillingProjectMonitorSupervisor._
  import context._

  self ! Init

  override def receive = {
    case Init => init pipeTo self
    case Start => for (i <- 1 to workerCount) startOne()
    case Status.Failure(t) => logger.error("error initializing billing project monitor", t)
  }

  def init =
    for {
      _ <- pubSubDao.createTopic(pubSubTopicName)
      _ <- pubSubDao.createSubscription(pubSubTopicName, pubSubSubscriptionName)
    } yield Start

  def startOne(): Unit = {
    logger.info("starting BillingProjectMonitorActor")
    actorOf(BillingProjectMonitor.props(pollInterval, pollIntervalJitter, pubSubDao, pubSubSubscriptionName, datasource))
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        logger.error("unexpected error in billing project monitor", e)
        // start one to replace the error, stop the errored child so that we also drop its mailbox (i.e. restart not good enough)
        startOne()
        Stop
      }
    }

}

object BillingProjectMonitor {
  case object StartMonitorPass

  sealed abstract class BillingProjectResult(ackId: String)
  final case class ReportMessage(projectName: RawlsBillingProjectName, ackId: String) extends BillingProjectResult(ackId = ackId)
  final case class FailToSynchronize(t: Throwable, ackId: String) extends BillingProjectResult(ackId = ackId)

  def props(
             pollInterval: FiniteDuration,
             pollIntervalJitter: FiniteDuration,
             pubSubDao: GooglePubSubDAO,
             pubSubSubscriptionName: String,
             datasource: SlickDataSource): Props =
    Props(new BillingProjectMonitorActor(pollInterval, pollIntervalJitter, pubSubDao, pubSubSubscriptionName, datasource))
}

class BillingProjectMonitorActor(
                                   val pollInterval: FiniteDuration,
                                   pollIntervalJitter: FiniteDuration,
                                   pubSubDao: GooglePubSubDAO,
                                   pubSubSubscriptionName: String,
                                   datasource: SlickDataSource)
  extends Actor
    with LazyLogging
    with FutureSupport {
  import BillingProjectMonitor._
  import context._

  self ! StartMonitorPass

  // fail safe in case this actor is idle too long but not too fast (1 second lower limit)
  setReceiveTimeout(max((pollInterval + pollIntervalJitter) * 10, 1 second))

  private def max(durations: FiniteDuration*): FiniteDuration = {
    implicit val finiteDurationIsOrdered = scala.concurrent.duration.FiniteDuration.FiniteDurationIsOrdered
    durations.max
  }

  override def receive = {
    case StartMonitorPass =>
      // start the process by pulling a message and sending it back to self
      pubSubDao.pullMessages(pubSubSubscriptionName, 1).map(_.headOption) pipeTo self

    case Some(message: PubSubMessage) =>
      logger.debug(s"received billing project message: $message")
      val projectName: RawlsBillingProjectName = parseMessage(message)
      datasource.inTransaction { dataAccess =>
        // save project updates
        dataAccess.rawlsBillingProjectQuery.updateBillingProjectStatus(projectName, CreationStatuses.Ready)
      }.map(_ => ReportMessage(projectName, message.ackId)) pipeTo self

    case None =>
      // there was no message to wait and try again
      val nextTime = org.broadinstitute.dsde.workbench.util.addJitter(pollInterval, pollIntervalJitter)
      system.scheduler.scheduleOnce(nextTime.asInstanceOf[FiniteDuration], self, StartMonitorPass)

    case ReportMessage(report, ackId) =>
      acknowledgeMessage(ackId).map(_ => StartMonitorPass) pipeTo self

    case FailToSynchronize(t, ackId) =>
      acknowledgeMessage(ackId).map(_ => StartMonitorPass) pipeTo self

    case Status.Failure(t) => throw t

    case ReceiveTimeout =>
      throw new RawlsException("BillingProjectMonitorActor has received no messages for too long")

    case x => logger.info(s"unhandled $x")
  }

  private def acknowledgeMessage(ackId: String): Future[Unit] =
    pubSubDao.acknowledgeMessagesById(pubSubSubscriptionName, Seq(ackId))

  private def parseMessage(message: PubSubMessage): RawlsBillingProjectName = {
    RawlsBillingProjectName(message.contents.parseJson.asJsObject.fields("project_name").asInstanceOf[JsString].value)
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        Escalate
      }
    }
}
