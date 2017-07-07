package org.broadinstitute.dsde.rawls.metrics

import com.google.api.client.http.GenericUrl
import scala.collection.JavaConverters._

/**
  * Google-specific instances for the [[Expansion]] typeclass.
  */
object GoogleExpansion {
  /**
    * Implicit expansion for a Google [[GenericUrl]].
    * Uses the URL path, replacing any slashes with periods.
    */
  implicit object GenericUrlExpansion extends Expansion[GenericUrl] {
    private val removeEmail = """(.*)@(.*)""".r   // ( ͡° ͜ʖ ͡°)
    private val removeToken = """token(.*)""".r

    override def makeName(url: GenericUrl): String = {
      url.getPathParts.asScala
        .map(path => removeEmail.replaceAllIn(path, _.group(1)))
        .map(path => removeToken.replaceAllIn(path, "token"))
        .mkString(".")
    }
  }
}
