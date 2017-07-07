package org.broadinstitute.dsde.rawls.metrics

import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.api.client.http.{HttpResponse, HttpResponseException}
import nl.grons.metrics.scala.{Counter, Timer}
import org.broadinstitute.dsde.rawls.metrics.Expansion._
import org.broadinstitute.dsde.rawls.metrics.GoogleExpansion._
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumented._
import org.broadinstitute.dsde.rawls.model.Subsystems.Subsystem

/**
  * Mixin trait for Google instrumentation.
  */
trait GoogleInstrumented extends WorkbenchInstrumented {

  protected implicit def googleCounters[A: GoogleSubsystemMapper]: GoogleCounters[A] =
      (request, responseOrException, isRetry) => {
        val base = ExpandedMetricBuilder.expand(SubsystemMetricKey, implicitly[GoogleSubsystemMapper[A]].subsystem)
        val baseRequest = base
          .expand(HttpMethodMetricKey, request.getRequestMethod.toLowerCase)
          .expand(UriPathMetricKey, request.buildHttpRequestUrl)
        val responseCounter = base
          .expand(HttpStatusMetricKey, responseOrException.fold(_.getStatusCode, _.getStatusCode))
          .asCounter("response")
        (baseRequest.asCounter(if (isRetry) "retry" else "request"), responseCounter, baseRequest.asTimer("latency"))
      }

  protected implicit def googleCountersWithSubsystem[A](subsystem: Subsystem): GoogleCounters[A] = {
    googleCounters[A](GoogleSubsystemMapper[A](subsystem))
  }
}

object GoogleInstrumented {
  type GoogleCounters[A] = (AbstractGoogleClientRequest[A], Either[HttpResponseException, HttpResponse], Boolean) => (Counter, Counter, Timer)
}