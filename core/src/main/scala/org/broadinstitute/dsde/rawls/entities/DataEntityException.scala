package org.broadinstitute.dsde.rawls.entities

import org.broadinstitute.dsde.rawls.RawlsException

/** Exception related to working with data entities.
 */
class DataEntityException(message: String = null, cause: Throwable = null) extends RawlsException(message, cause)

