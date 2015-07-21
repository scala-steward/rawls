package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.ExecutionServiceStatus
import spray.http.HttpCookie

import scala.concurrent.Future

/**
 * @author tsharpe
 */
trait ExecutionServiceDAO {
  def submitWorkflow( wdl: String, inputs: String, authCookie: HttpCookie ): ExecutionServiceStatus
  def status(id: String, authCookie: HttpCookie): Future[ExecutionServiceStatus]
}
