package org.broadinstitute.dsde.rawls.entities.datarepo

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import bio.terra.datarepo.model.TableModel
import bio.terra.workspace.model.DataReferenceDescription.ReferenceTypeEnum
import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import com.google.cloud.bigquery.{QueryJobConfiguration, QueryParameterValue, TableResult}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, SamDAO}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionValidator}
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, EntityTypeNotFoundException, UnsupportedEntityOperationException}
import org.broadinstitute.dsde.rawls.expressions.Transformers
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.{AntlrTerraExpressionParser, DataRepoEvaluateToAttributeVisitor, ParsedDataRepoExpression}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport.TerraDataRepoSnapshotRequestFormat
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, AttributeNull, AttributeValue, AttributeValueEmptyList, AttributeValueList, AttributeValueRawJson, DataReferenceName, Entity, EntityTypeMetadata, ErrorReport, SubmissionValidationEntityInputs, SubmissionValidationValue, TerraDataRepoSnapshotRequest}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import spray.json._
import Transformers._
import cromwell.client.model.ToolInputParameter
import cromwell.client.model.ValueType.TypeNameEnum
import org.broadinstitute.dsde.rawls.util.CollectionUtils

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class DataRepoEntityProvider(requestArguments: EntityRequestArguments, workspaceManagerDAO: WorkspaceManagerDAO,
                             dataRepoDAO: DataRepoDAO, samDAO: SamDAO, bqServiceFactory: GoogleBigQueryServiceFactory)
                            (implicit protected val executionContext: ExecutionContext)
  extends EntityProvider with DataRepoBigQuerySupport with LazyLogging {

  val workspace = requestArguments.workspace
  val userInfo = requestArguments.userInfo
  val dataReferenceName = requestArguments.dataReference.getOrElse(throw new DataEntityException("data reference must be defined for this provider"))
  val datarepoRowIdColumn = "datarepo_row_id"


  private lazy val snapshotModel = {
    // get snapshot UUID from data reference name
    val snapshotId = lookupSnapshotForName(dataReferenceName)

    // contact TDR to describe the snapshot
    dataRepoDAO.getSnapshot(snapshotId, userInfo.accessToken)
  }

  private lazy val googleProject = {
    // determine project to be billed for the BQ job TODO: need business logic from PO!
    requestArguments.billingProject match {
      case Some(billing) => billing.projectName.value
      case None => workspace.namespace
    }
  }


  override def entityTypeMetadata(): Future[Map[String, EntityTypeMetadata]] = {

    // TODO: AS-321 auto-switch to see if the ref supplied in argument is a UUID or a name?? Use separate query params? Never allow ID?

    // reformat TDR's response into the expected response structure
    val entityTypesResponse: Map[String, EntityTypeMetadata] = snapshotModel.getTables.asScala.map { table =>
      val attrs: Seq[String] = table.getColumns.asScala.map(_.getName)
      val primaryKey = pkFromSnapshotTable(table)
      (table.getName, EntityTypeMetadata(table.getRowCount, primaryKey, attrs))
    }.toMap

    Future.successful(entityTypesResponse)

  }

  override def createEntity(entity: Entity): Future[Entity] =
    throw new UnsupportedEntityOperationException("create entity not supported by this provider.")

  override def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int] =
    throw new UnsupportedEntityOperationException("delete entities not supported by this provider.")


  override def getEntity(entityType: String, entityName: String): Future[Entity] = {
    // extract table definition, with PK, from snapshot schema
    val tableModel = getTableModel(entityType)

    //  determine pk column
    val pk = pkFromSnapshotTable(tableModel)
    // determine data project
    val dataProject = snapshotModel.getDataProject
    // determine view name
    val viewName = snapshotModel.getName
    // generate BQ SQL for this entity
    val query = s"SELECT * FROM `${dataProject}.${viewName}.${entityType}` WHERE $pk = @pkvalue;"
    // generate query config, with named param for primary key
    val queryConfig = QueryJobConfiguration.newBuilder(query)
      .addNamedParameter("pkvalue", QueryParameterValue.string(entityName))
      .build

    // get pet service account key for this user
    samDAO.getPetServiceAccountKeyForUser(googleProject, requestArguments.userInfo.userEmail) map { petKey =>

      // get a BQ service (i.e. dao) instance, and use it to execute the query against BQ
      val queryResource: Resource[IO, TableResult] = for {
        bqService <- bqServiceFactory.getServiceForPet(petKey)
        queryResults <- Resource.liftF(bqService.query(queryConfig))
      } yield queryResults

      // translate the BQ results into a single Rawls Entity
      queryResource.use { queryResults: TableResult =>
        IO.pure(queryResultsToEntity(queryResults, entityType, pk))
      }.unsafeRunSync()

    }
  }

  private def getTableModel(entityType: String) = {
    snapshotModel.getTables.asScala.find(_.getName == entityType) match {
      case Some(table) => table
      case None => throw new EntityTypeNotFoundException(entityType)
    }
  }

  // not marked as private to ease unit testing
  def lookupSnapshotForName(dataReferenceName: DataReferenceName): UUID = {
    // contact WSM to retrieve the data reference specified in the request
    val dataRef = workspaceManagerDAO.getDataReferenceByName(UUID.fromString(workspace.workspaceId),
      ReferenceTypeEnum.DATAREPOSNAPSHOT.getValue,
      dataReferenceName,
      userInfo.accessToken)

    // the above request will throw a 404 if the reference is not found, so we can assume we have one by the time we reach here.

    // verify it's a TDR snapshot. should be a noop, since getDataReferenceByName enforces this.
    if (ReferenceTypeEnum.DATAREPOSNAPSHOT != dataRef.getReferenceType) {
      throw new DataEntityException(s"Reference type value for $dataReferenceName is not of type ${ReferenceTypeEnum.DATAREPOSNAPSHOT.getValue}")
    }

    // parse the raw reference value into a snapshot reference
    val dataReference = Try(dataRef.getReference.parseJson.convertTo[TerraDataRepoSnapshotRequest]) match {
      case Success(ref) => ref
      case Failure(err) => throw new DataEntityException(s"Could not parse reference value for $dataReferenceName: ${err.getMessage}", err)
    }

    // verify the instance matches our target instance
    // TODO: AS-321 is this the right place to validate this? We could add a "validateInstanceURL" method to the DAO itself, for instance
    if (!dataReference.instanceName.equalsIgnoreCase(dataRepoDAO.getInstanceName)) {
      logger.error(s"expected instance name ${dataRepoDAO.getInstanceName}, got ${dataReference.instanceName}")
      throw new DataEntityException(s"Reference value for $dataReferenceName contains an unexpected instance name value")
    }

    // verify snapshotId value is a UUID
    Try(UUID.fromString(dataReference.snapshot)) match {
      case Success(uuid) => uuid
      case Failure(ex) =>
        logger.error(s"invalid UUID for snapshotId in reference: ${dataReference.snapshot}")
        throw new DataEntityException(s"Reference value for $dataReferenceName contains an unexpected snapshot value", ex)
    }

  }

  def pkFromSnapshotTable(tableModel: TableModel, tableAlias: Option[String] = None): String = {
    // If data repo returns one and only one primary key, use it.
    // If data repo returns null or a compound PK, use the built-in rowid for pk instead.
    val pkColumn = scala.Option(tableModel.getPrimaryKey) match {
      case Some(pk) if pk.size() == 1 => pk.asScala.head
      case _ => datarepoRowIdColumn // default data repo value
    }
    tableAlias.map(alias => s"$alias.$pkColumn").getOrElse(pkColumn)
  }

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext, gatherInputsResult: GatherInputsResult): Future[Stream[SubmissionValidationEntityInputs]] = {
    expressionEvaluationContext match {
      case ExpressionEvaluationContext(None, None, None, Some(rootEntityType)) =>
        implicit val contextShift: ContextShift[IO] = IO.contextShift(executionContext)
        val baseTableAlias = "root"
        val resultIO = for {
          parsedExpressions <- parseAllExpressions(gatherInputsResult, baseTableAlias)
          tableModel = getTableModel(rootEntityType)
          _ <- assertDataNotTooBig(parsedExpressions, tableModel)
          entityNameColumn = pkFromSnapshotTable(tableModel, Option(baseTableAlias))
          bqQueryJobConfigs = generateBigQueryJobConfigs(parsedExpressions, tableModel, entityNameColumn, baseTableAlias)
          petKey <- IO.fromFuture(IO(samDAO.getPetServiceAccountKeyForUser(googleProject, requestArguments.userInfo.userEmail)))
          expressionResults <- runBigQueryQueries(entityNameColumn, bqQueryJobConfigs, petKey)
        } yield {
          val groupedResults = groupResultsByExpressionAndEntityName(expressionResults)
          val rootEntities = expressionResults.flatMap {
            case (_, resultsMap) => resultsMap.keys
          }.distinct

          val entityNameAndInputValues = constructInputsForEachEntity(gatherInputsResult, groupedResults, baseTableAlias, rootEntities)

          CollectionUtils.groupByTuples(entityNameAndInputValues)
            .map({ case (entityName, values) => SubmissionValidationEntityInputs(entityName, values.toSet) }).toStream
        }
        resultIO.unsafeToFuture()

      case _ => Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Only root entity type supported for Data Repo workflows")))
    }
  }

  private def assertDataNotTooBig(parsedExpressions: Set[ParsedDataRepoExpression], tableModel: TableModel) = {
    if (tableModel.getRowCount * parsedExpressions.size > 1000000) {
      // todo: configure this number 100000 AND decide what to tell the user as a mitigation
      IO.raiseError(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Too many results. Choose a table with fewer rows or a workflow configuration with fewer inputs.")))
    } else {
      IO.unit
    }
  }

  private def constructInputsForEachEntity(gatherInputsResult: GatherInputsResult, groupedResults: Seq[(Transformers.LookupExpression, Map[Transformers.EntityName, Try[scala.Iterable[AttributeValue]]])], baseTableAlias: LookupExpression, rootEntities: List[EntityName]) = {
    // gatherInputsResult.processableInputs.toSeq so that the result is not a Set and does not worry about duplicates
    gatherInputsResult.processableInputs.toSeq.flatMap { input =>
      val parser = AntlrTerraExpressionParser.getParser(input.expression)
      val visitor = new DataRepoEvaluateToAttributeVisitor(baseTableAlias)
      val parsedTree = parser.root()
      val lookupExpressions = visitor.visit(parsedTree).map {
        _.expression
      }

      val transformers = new Transformers(Option(rootEntities))
      val expressionResultsByEntityName = transformers.transformAndParseExpr(groupedResults.filter {
        case (expression, _) => lookupExpressions.contains(expression)
      }, parsedTree)

      val validationValuesByEntity: Seq[(EntityName, SubmissionValidationValue)] = expressionResultsByEntityName.map {
        case (key, Success(attrSeq)) => key -> unpackResult(attrSeq.toSeq, input.workflowInput)
        case (key, Failure(regret)) => key -> SubmissionValidationValue(None, Some(regret.getMessage), input.workflowInput.getName)
      }.toSeq
      validationValuesByEntity
    }
  }

  private def groupResultsByExpressionAndEntityName(expressionResults: List[(LookupExpression, Map[EntityName, Try[Iterable[AttributeValue]]])]) = {
    expressionResults.groupBy {
      case (expression, _) => expression
    }.toSeq.map {
      case (expression, groupedList) => (expression, groupedList.foldLeft(Map.empty[EntityName, Try[Iterable[AttributeValue]]]) {
        case (aggregateResults, (_, individualResult)) => aggregateResults ++ individualResult
      })
    }
  }

  private def runBigQueryQueries(entityNameColumn: String, bqQueryJobConfigs: Map[Set[ParsedDataRepoExpression], QueryJobConfiguration], petKey: String) = {
    (for {
      bqService <- bqServiceFactory.getServiceForPet(petKey)
      queryResults <- Resource.liftF(bqQueryJobConfigs.toList.traverse { // should we do parTraverse?
        case (expressions, bqJob) => bqService.query(bqJob).map(expressions -> _)
      })
    } yield {
      if (queryResults.exists { case (_, tableResults) => tableResults.getTotalRows > 1000000 }) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Too many results. This is likely a large one to many relationship.")) // todo: configure this number 100000 AND decide what to tell the user as a mitigation
      }
      for {
        (parsedExpressions, tableResult) <- queryResults
        resultRow <- tableResult.iterateAll().asScala
        parsedExpression <- parsedExpressions
      } yield {
        val field = tableResult.getSchema.getFields.get(parsedExpression.qualifiedColumnName) // is case sensitivity an issue on columnName?
        val primaryKey: EntityName = resultRow.get(entityNameColumn).getStringValue
        val attribute = fieldToAttribute(field, resultRow)
        val evaluationResult: Try[Iterable[AttributeValue]] = attribute match {
          case v: AttributeValue => Success(Seq(v))
          case AttributeValueList(l) => Success(l)
          case unsupported => Failure(new RawlsException(s"unsupported attribute: $unsupported"))
        }
        (parsedExpression.expression, Map(primaryKey -> evaluationResult))
      }
    }).use(IO.pure)
  }

  private def generateBigQueryJobConfigs(parsedExpressions: Set[ParsedDataRepoExpression], tableModel: TableModel, entityNameColumn: String, baseTableAlias: String) = {
    parsedExpressions.groupBy(_.relationships).map {
      case (Nil, expressions) =>
        val columnNames = expressions.map(_.columnName)
        val validColumnNames = tableModel.getColumns.asScala.map(_.getName.toLowerCase).toSet

        val invalidColumnNames = columnNames -- validColumnNames
        if (invalidColumnNames.nonEmpty) {
          // we should have validated all this already, this is just to be sure we don't get any sql injection
          throw new RawlsException(s"invalid columns: ${invalidColumnNames.mkString(",")}")
        }

        val dataProject = snapshotModel.getDataProject
        // determine view name
        val viewName = snapshotModel.getName
        // generate BQ SQL for this entity
        val query = s"SELECT $entityNameColumn, ${expressions.map(_.qualifiedColumnName).mkString(", ")} FROM `${dataProject}.${viewName}.${tableModel.getName}` $baseTableAlias"

        (expressions, QueryJobConfiguration.newBuilder(query).build)

      case _ => throw new RawlsException("relations not implemented yet")
    }
  }

  private def parseAllExpressions(gatherInputsResult: GatherInputsResult, baseTableAlias: LookupExpression): IO[Set[ParsedDataRepoExpression]] = IO {
    gatherInputsResult.processableInputs.flatMap { input =>
      val parser = AntlrTerraExpressionParser.getParser(input.expression)
      val visitor = new DataRepoEvaluateToAttributeVisitor(baseTableAlias)
      visitor.visit(parser.root())
    }
  }

  override def expressionValidator: ExpressionValidator =
    throw new UnsupportedEntityOperationException("expressionEvaluator not supported by this provider.")


  private def unpackResult(mcSequence: Iterable[AttributeValue], wfInput: ToolInputParameter): SubmissionValidationValue = wfInput.getValueType.getTypeName match {
    case TypeNameEnum.ARRAY => getArrayResult(wfInput.getName, mcSequence)
    case TypeNameEnum.OPTIONAL  => if (wfInput.getValueType.getOptionalType.getTypeName == TypeNameEnum.ARRAY)
      getArrayResult(wfInput.getName, mcSequence)
    else getSingleResult(wfInput.getName, mcSequence, wfInput.getOptional) //send optional-arrays down the same codepath as arrays
    case _ => getSingleResult(wfInput.getName, mcSequence, wfInput.getOptional)
  }


  private val emptyResultError = "Expected single value for workflow input, but evaluated result set was empty"
  private val multipleResultError  = "Expected single value for workflow input, but evaluated result set had multiple values"

  private def getSingleResult(inputName: String, seq: Iterable[AttributeValue], optional: Boolean): SubmissionValidationValue = {
    def handleEmpty = if (optional) None else Some(emptyResultError)
    seq match {
      case Seq() => SubmissionValidationValue(None, handleEmpty, inputName)
      case Seq(null) => SubmissionValidationValue(None, handleEmpty, inputName)
      case Seq(AttributeNull) => SubmissionValidationValue(None, handleEmpty, inputName)
      case Seq(singleValue) => SubmissionValidationValue(Some(singleValue), None, inputName)
      case multipleValues => SubmissionValidationValue(Some(AttributeValueList(multipleValues.toSeq)), Some(multipleResultError), inputName)
    }
  }

  private def getArrayResult(inputName: String, seq: Iterable[AttributeValue]): SubmissionValidationValue = {
    val notNull: Seq[AttributeValue] = seq.filter(v => v != null && v != AttributeNull).toSeq
    val attr = notNull match {
      case Nil => Option(AttributeValueEmptyList)
      //GAWB-2509: don't pack single-elem RawJson array results into another layer of array
      //NOTE: This works, except for the following situation: a participant with a RawJson double-array attribute, in a single-element participant set.
      // Evaluating this.participants.raw_json on the pset will incorrectly hit this case and return a 2D array when it should return a 3D array.
      // The true fix for this is to look into why the slick expression evaluator wraps deserialized AttributeValues in a Seq, and instead
      // return the proper result type, removing the need to infer whether it's a scalar or array type from the WDL input.
      case AttributeValueRawJson(JsArray(_)) +: Seq() => Option(notNull.head)
      case _ => Option(AttributeValueList(notNull))
    }
    SubmissionValidationValue(attr, None, inputName)
  }

}
