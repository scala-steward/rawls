package org.broadinstitute.dsde.rawls.entities.base

import org.broadinstitute.dsde.rawls.expressions.Transformers.LookupExpression
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, AttributeValue, Entity, EntityTypeMetadata, SubmissionValidationEntityInputs}

import scala.concurrent.Future
import scala.util.Try

/**
 * trait definition for entity providers.
 */
trait EntityProvider {

  def entityTypeMetadata(): Future[Map[String, EntityTypeMetadata]]

  def createEntity(entity: Entity): Future[Entity]

  def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int]

  def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext, gatherInputsResult: GatherInputsResult, workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]): Future[Stream[SubmissionValidationEntityInputs]]

  def expressionValidator: ExpressionValidator

  def getEntity(entityType: String, entityName: String): Future[Entity]
}
