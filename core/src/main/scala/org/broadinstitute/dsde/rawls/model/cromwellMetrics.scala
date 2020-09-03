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

case class CromwellMetricsBqTable(name: String, fields: List[(String, String)])

case class CromwellMetricsBqSchema
(
  version: String,
  additionalTables: List[CromwellMetricsBqTable],
)

object CromwellMetricsBqSchema {
  val TimestampFieldName = "timestamp"
  val TimestampFieldType = "TIMESTAMP"
  val TimestampFieldMode = "REQUIRED"

  val SubmissionIdFieldName = "submission_id"
  val WorkspaceIdFieldName = "workspace_id"
  val WorkspaceNamespaceFieldName = "workspace_namespace"
  val WorkspaceNameFieldName = "workspace_name"
  val SchemaVersionMajorFieldName = "schema_version_major"
  val SchemaVersionMinorFieldName = "schema_version_minor"
  val SchemaVersionPatchFieldName = "schema_version_patch"

  val PerSubmissionTableName = "per_submission"
  val PerSubmissionTableFields = List(
    SubmissionIdFieldName -> "STRING",
    WorkspaceIdFieldName -> "STRING",
    WorkspaceNamespaceFieldName -> "STRING",
    WorkspaceNameFieldName -> "STRING",
    SchemaVersionMajorFieldName -> "INT64",
    SchemaVersionMinorFieldName -> "INT64",
    SchemaVersionPatchFieldName -> "INT64",
  )

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
      additionalTables =
        additionalTableNames
          .map(
            tableName =>
              CromwellMetricsBqTable(
                name = tableName,
                fields = additionalTablesConfig
                  .as[Map[String, String]](tableName)
                  .toList
                  .sortBy {
                    case (name, _) => name
                  },
              )
          ),
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
