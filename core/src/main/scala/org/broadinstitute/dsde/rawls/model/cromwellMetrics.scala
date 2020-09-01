package org.broadinstitute.dsde.rawls.model

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.rawls.RawlsFatalExceptionWithErrorReport
import scala.collection.JavaConverters._

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
  perSubmissionFields: Map[String, String],
  additionalTables: List[CromwellMetricsBqTable],
)

object CromwellMetricsBqSchema {
  val PerSubmissionTable = "per_submission"

  def fromConfig(config: Config): CromwellMetricsBqSchema = {
    val additionalTablesConfig = config.getConfig("additional_tables")
    val additionalTableNames =
      additionalTablesConfig
        .entrySet()
        .asScala
        .map(_.getKey.split("\\.").toSeq.head)
        .toList

    CromwellMetricsBqSchema(
      version = config.getString("version"),
      perSubmissionFields = config.as[Map[String, String]]("per_submission_table"),
      additionalTables =
        additionalTableNames
          .map(tableName => CromwellMetricsBqTable.fromConfig(tableName, additionalTablesConfig.getConfig(tableName))),
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

case class CromwellMetricsBqTable(name: String, fields: Map[String, String])

object CromwellMetricsBqTable {
  def fromConfig(tableName: String, tableConfig: Config): CromwellMetricsBqTable = {
    CromwellMetricsBqTable(
      name = tableName,
      fields = tableConfig.as[Map[String, String]](tableName),
    )
  }
}
