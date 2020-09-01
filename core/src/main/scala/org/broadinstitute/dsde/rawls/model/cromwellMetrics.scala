package org.broadinstitute.dsde.rawls.model

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.rawls.RawlsFatalExceptionWithErrorReport

case class CromwellMetrics
(
  policy: String,
  schema: CromwellMetricsBqSchema,
)

object CromwellMetrics {
  def fromConfig(config: Config): Option[CromwellMetrics] = {
    config.getAs[Boolean]("enabled").filter(_ == true) map { _ =>
      CromwellMetrics(
        policy = config.getString("policy"),
        schema = CromwellMetricsBqSchema.fromConfig(config.getConfig("schema")),
      )
    }
  }
}

case class CromwellMetricsBqSchema
(
  version: String,
  perSubmission: Map[String, String],
  perJob: Map[String, String],
  perMetric: Map[String, String],
)

object CromwellMetricsBqSchema {
  val PerSubmissionTable = "per_submission"
  val PerJob = "per_job"
  val PerMetricTable = "per_metric"

  def fromConfig(config: Config): CromwellMetricsBqSchema = {
    CromwellMetricsBqSchema(
      version = config.getString("version"),
      perSubmission = config.as[Map[String, String]]("per_submission"),
      perJob = config.as[Map[String, String]]("per_job"),
      perMetric = config.as[Map[String, String]]("per_metric"),
    )
  }

  def parseVersion(version: String): (Int, Int, Int) = {
    try {
      version.split("\\.").map(_.toInt).toList match {
        case major :: minor :: patch :: Nil => (major, minor, patch)
        case _ => sys.error(s"Unexpected version format: '$version'")
      }
    } catch {
      case exception: Exception =>
        throw new RawlsFatalExceptionWithErrorReport(
          ErrorReport(s"Invalid version: $version", ErrorReport(exception))
        )
    }
  }
}
