package org.broadinstitute.dsde.rawls.entities.local

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, EntityRecord, ReadWriteAction}
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionEvaluationSupport, ExpressionValidator}
import org.broadinstitute.dsde.rawls.entities.exceptions.DeleteEntitiesConflictException
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.expressions.Transformers.LookupExpression
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.{GatherInputsResult, MethodInput}
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, AttributeValue, Entity, EntityTypeMetadata, ErrorReport, SubmissionValidationEntityInputs, SubmissionValidationValue, Workspace}
import org.broadinstitute.dsde.rawls.util.{CollectionUtils, EntitySupport}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * Terra default entity provider, powered by Rawls and Cloud SQL
 */
class LocalEntityProvider(workspace: Workspace, implicit protected val dataSource: SlickDataSource)
                         (implicit protected val executionContext: ExecutionContext)
  extends EntityProvider with LazyLogging with EntitySupport with ExpressionEvaluationSupport {

  import dataSource.dataAccess.driver.api._

  private val workspaceContext = workspace

  override def entityTypeMetadata(): Future[Map[String, EntityTypeMetadata]] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.entityQuery.getEntityTypeMetadata(workspaceContext)
    }
  }

  override def createEntity(entity: Entity): Future[Entity] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.entityQuery.get(workspaceContext, entity.entityType, entity.name) flatMap {
        case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"${entity.entityType} ${entity.name} already exists in ${workspace.toWorkspaceName}")))
        case None => dataAccess.entityQuery.save(workspaceContext, entity)
      }
    }
  }

  // EntityApiServiceSpec has good test coverage for this api
  override def deleteEntities(entRefs: Seq[AttributeEntityReference]): Future[Int] = {
    dataSource.inTransaction { dataAccess =>
      // withAllEntities throws exception if some entities not found; passes through if all ok
      withAllEntities(workspaceContext, dataAccess, entRefs) { entities =>
        dataAccess.entityQuery.getAllReferringEntities(workspaceContext, entRefs.toSet) flatMap { referringEntities =>
          if (referringEntities != entRefs.toSet)
            throw new DeleteEntitiesConflictException(referringEntities)
          else {
            dataAccess.entityQuery.hide(workspaceContext, entRefs)
          }
        }
      }
    }
  }

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext, gatherInputsResult: GatherInputsResult, workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]): Future[Stream[SubmissionValidationEntityInputs]] = {
    dataSource.inTransaction { dataAccess =>
      withEntityRecsForExpressionEval(expressionEvaluationContext, workspace, dataAccess) { jobEntityRecs =>
        //Parse out the entity -> results map to a tuple of (successful, failed) SubmissionValidationEntityInputs
        evaluateExpressionsInternal(workspace, gatherInputsResult.processableInputs, jobEntityRecs, dataAccess) map { valuesByEntity =>
          createSubmissionValidationEntityInputs(valuesByEntity)
        }
      }
    }
  }

  override def expressionValidator: ExpressionValidator = new LocalEntityExpressionValidator

  protected[local] def evaluateExpressionsInternal(workspaceContext: Workspace, inputs: Set[MethodInput], entities: Option[Seq[EntityRecord]], dataAccess: DataAccess)(implicit executionContext: ExecutionContext): ReadWriteAction[Map[String, Seq[SubmissionValidationValue]]] = {
    import dataAccess.driver.api._

    val entityNames = entities match {
      case Some(recs) => recs.map(_.name)
      case None => Seq("")
    }

    if( inputs.isEmpty ) {
      //no inputs to evaluate = just return an empty map back!
      DBIO.successful(entityNames.map( _ -> Seq.empty[SubmissionValidationValue] ).toMap)
    } else {
      ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, entities) { evaluator =>
        //Evaluate the results per input and return a seq of DBIO[ Map(entity -> value) ], one per input
        val resultsByInput = inputs.toSeq.map { input =>
          evaluator.evalFinalAttribute(workspaceContext, input.expression).asTry.map { tryAttribsByEntity =>
            val validationValuesByEntity: Seq[(String, SubmissionValidationValue)] = tryAttribsByEntity match {
              case Failure(regret) =>
                //The DBIOAction failed - this input expression was not evaluated. Make an error for each entity.
                entityNames.map((_, SubmissionValidationValue(None, Some(regret.getMessage), input.workflowInput.getName)))
              case Success(attributeMap) =>
                convertToSubmissionValidationValues(attributeMap, input)
            }
            validationValuesByEntity
          }
        }

        //Flip the list of DBIO monads into one on the outside that we can map across and then group by entity.
        DBIO.sequence(resultsByInput) map { results =>
          CollectionUtils.groupByTuples(results.flatten)
        }
      }
    }
  }

  override def getEntity(entityType: String, entityName: String): Future[Entity] = {
    dataSource.inTransaction { dataAccess =>
      withEntity(workspaceContext, entityType, entityName, dataAccess) {
        entity => DBIO.successful(entity)
      }
    }
  }

}
