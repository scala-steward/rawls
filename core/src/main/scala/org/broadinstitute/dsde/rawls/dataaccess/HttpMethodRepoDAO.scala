package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.MethodRepoJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.{AgoraEntity, AgoraEntityType, AgoraStatus, MethodConfiguration, Subsystems, UserInfo}
import org.broadinstitute.dsde.rawls.util.HttpUtils._
import org.broadinstitute.dsde.rawls.util.Retry
import spray.client.pipelining._
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.httpx.UnsuccessfulResponseException
import spray.httpx.unmarshalling.FromResponseUnmarshaller
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/**
 * @author tsharpe
 */
class HttpMethodRepoDAO(baseMethodRepoServiceURL: String, apiPath: String = "", override val workbenchMetricBaseName: String)(implicit val system: ActorSystem) extends MethodRepoDAO with DsdeHttpDAO with Retry with LazyLogging with RawlsInstrumented {
  import system.dispatcher

  private val methodRepoServiceURL = baseMethodRepoServiceURL + apiPath

  private lazy implicit val baseMetricBuilder: ExpandedMetricBuilder =
    ExpandedMetricBuilder.expand(SubsystemMetricKey, Subsystems.Agora)

  private def pipeline[A: FromResponseUnmarshaller](userInfo: UserInfo, retryCount: Int = 0)(request: HttpRequest) =
    (addAuthHeader(userInfo) ~> instrument(gzSendReceive) ~> unmarshal[A]) apply request

  private def getAgoraEntity( uri: Uri, userInfo: UserInfo ): Future[Option[AgoraEntity]] = {
    retry(when500) { count => pipeline[Option[AgoraEntity]](userInfo, count)(Get(uri)) } recover {
      case notOK: UnsuccessfulResponseException if StatusCodes.NotFound == notOK.response.status => None
    }
  }

  private def postAgoraEntity( uri: Uri, agoraEntity: AgoraEntity, userInfo: UserInfo): Future[AgoraEntity] = {
    retry(when500) { count => pipeline[AgoraEntity](userInfo, count)(Post(uri, agoraEntity)) }
  }

  override def getMethodConfig( namespace: String, name: String, version: Int, userInfo: UserInfo ): Future[Option[AgoraEntity]] = {
    getAgoraEntity(s"${methodRepoServiceURL}/configurations/${namespace}/${name}/${version}",userInfo)
  }

  override def getMethod( namespace: String, name: String, version: Int, userInfo: UserInfo ): Future[Option[AgoraEntity]] = {
    getAgoraEntity(s"${methodRepoServiceURL}/methods/${namespace}/${name}/${version}",userInfo)
  }

  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case ure: spray.client.UnsuccessfulResponseException => ure.responseStatus.intValue / 100 == 5
      case ure: spray.httpx.UnsuccessfulResponseException => ure.response.status.intValue / 100 == 5
      case _ => false
    }
  }

  override def postMethodConfig(namespace: String, name: String, methodConfiguration: MethodConfiguration, userInfo: UserInfo): Future[AgoraEntity] = {
    val agoraEntity = AgoraEntity(
      namespace = Option(namespace),
      name = Option(name),
      payload = Option(methodConfiguration.toJson.toString),
      entityType = Option(AgoraEntityType.Configuration)
    )
    postAgoraEntity(s"${methodRepoServiceURL}/configurations", agoraEntity, userInfo)
  }

  override def getStatus(implicit executionContext: ExecutionContext): Future[AgoraStatus] = {
    val url = s"${baseMethodRepoServiceURL}/status"
    val pipeline = sendReceive ~> unmarshal[AgoraStatus]
    // Don't retry on the status check
    pipeline(Get(url)) recover {
      case NonFatal(e) => AgoraStatus(false, Seq(e.getMessage))
    }
  }

}