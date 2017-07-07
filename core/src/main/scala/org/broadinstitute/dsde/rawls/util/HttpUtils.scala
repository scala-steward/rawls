package org.broadinstitute.dsde.rawls.util

import akka.actor.ActorRefFactory
import akka.util.Timeout
import nl.grons.metrics.scala.{Counter, Timer}
import spray.client.pipelining.{sendReceive, _}
import spray.http.HttpEncodings.gzip
import spray.http.HttpHeaders.`Accept-Encoding`
import spray.http.{HttpRequest, HttpResponse, StatusCode}
import spray.httpx.encoding.Gzip

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
  * Created by rtitle on 7/7/17.
  */
object HttpUtils {

  //sendReceive, but with gzip.
  //Two versions, one of which overrides the timeout.
  def gzSendReceive(timeout: Timeout)(implicit actorRefFactory: ActorRefFactory, executionContext: ExecutionContext) = {
    implicit val to = timeout //make the explicit implicit, a process known as "tactfulization"
    addHeaders(`Accept-Encoding`(gzip)) ~> sendReceive ~> decode(Gzip)
  }

  def gzSendReceive(implicit actorRefFactory: ActorRefFactory, executionContext: ExecutionContext) = addHeaders(`Accept-Encoding`(gzip)) ~> sendReceive ~> decode(Gzip)

  def countRequest(requestCounter: HttpRequest => Counter): RequestTransformer = request => {
    requestCounter(request) += 1
    request
  }

  def countResponse(responseCounter: HttpResponse => Counter): ResponseTransformer = response => {
    responseCounter(response) += 1
    response
  }

  def timedSendReceive(requestTimer: HttpRequest => Timer, sr: SendReceive)(implicit actorRefFactory: ActorRefFactory, executionContext: ExecutionContext): SendReceive = {
    request => (sr andThen { future => requestTimer(request).timeFuture(future) })(request)
  }

  def instrument(sendReceive: SendReceive)(implicit requestCounter: (HttpRequest, Boolean) => Counter, responseCounter: HttpResponse => Counter, requestTimer: HttpRequest => Timer, retryCount: Int = 0, actorRefFactory: ActorRefFactory, executionContext: ExecutionContext): SendReceive = {
    countRequest(requestCounter(_, retryCount != 0)) ~> timedSendReceive(requestTimer, sendReceive) ~> countResponse(responseCounter)
  }

  def instrumentedSendReceive(implicit requestCounter: (HttpRequest, Boolean) => Counter, responseCounter: HttpResponse => Counter, requestTimer: HttpRequest => Timer, retryCount: Int = 0, actorRefFactory: ActorRefFactory, executionContext: ExecutionContext): SendReceive = {
    instrument(sendReceive)
  }

}