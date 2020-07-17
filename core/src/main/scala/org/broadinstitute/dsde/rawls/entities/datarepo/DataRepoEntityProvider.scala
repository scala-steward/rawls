package org.broadinstitute.dsde.rawls.entities.datarepo

import akka.http.scaladsl.model.StatusCodes
import bio.terra.datarepo.model.{SnapshotModel, TableModel}
import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import com.google.cloud.bigquery.{QueryJobConfiguration, QueryParameterValue, TableResult}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, SamDAO}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionEvaluationSupport, ExpressionValidator}
import org.broadinstitute.dsde.rawls.entities.exceptions.{EntityTypeNotFoundException, UnsupportedEntityOperationException}
import org.broadinstitute.dsde.rawls.expressions.Transformers
import org.broadinstitute.dsde.rawls.expressions.Transformers.{EntityName, ExpressionAndResult, LookupExpression}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.{AntlrTerraExpressionParser, DataRepoEvaluateToAttributeVisitor, LookupExpressionExtractionVisitor, ParsedDataRepoExpression}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, AttributeValue, AttributeValueList, Entity, EntityTypeMetadata, ErrorReport, SubmissionValidationEntityInputs, SubmissionValidationValue}
import org.broadinstitute.dsde.rawls.util.CollectionUtils
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class DataRepoEntityProvider(snapshotModel: SnapshotModel, requestArguments: EntityRequestArguments,
                             samDAO: SamDAO, bqServiceFactory: GoogleBigQueryServiceFactory,
                             config: DataRepoEntityProviderConfig)
                            (implicit protected val executionContext: ExecutionContext)
  extends EntityProvider with DataRepoBigQuerySupport with LazyLogging with ExpressionEvaluationSupport {

  val datarepoRowIdColumn = "datarepo_row_id"

  private lazy val googleProject = {
    // determine project to be billed for the BQ job TODO: need business logic from PO!
    requestArguments.billingProject match {
      case Some(billing) => billing.projectName.value
      case None => requestArguments.workspace.namespace
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

  def pkFromSnapshotTable(tableModel: TableModel): String = {
    // If data repo returns one and only one primary key, use it.
    // If data repo returns null or a compound PK, use the built-in rowid for pk instead.
    scala.Option(tableModel.getPrimaryKey) match {
      case Some(pk) if pk.size() == 1 => pk.asScala.head
      case _ => datarepoRowIdColumn // default data repo value
    }
  }

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext, gatherInputsResult: GatherInputsResult, workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]): Future[Stream[SubmissionValidationEntityInputs]] = {
    expressionEvaluationContext match {
      case ExpressionEvaluationContext(None, None, None, Some(rootEntityType)) =>
        implicit val contextShift: ContextShift[IO] = IO.contextShift(executionContext)
        val baseTableAlias = "root"
        val resultIO = for {
          parsedExpressions <- parseAllExpressions(gatherInputsResult, baseTableAlias)
          tableModel = getTableModel(rootEntityType)
          _ <- checkSubmissionSize(parsedExpressions, tableModel)
          entityNameColumn = pkFromSnapshotTable(tableModel)
          bqQueryJobConfigs = generateBigQueryJobConfigs(parsedExpressions, tableModel, entityNameColumn, baseTableAlias)
          petKey <- IO.fromFuture(IO(samDAO.getPetServiceAccountKeyForUser(googleProject, requestArguments.userInfo.userEmail)))
          bqExpressionResults <- runBigQueryQueries(entityNameColumn, bqQueryJobConfigs, petKey)
          rootEntities = bqExpressionResults.flatMap {
            case (_, resultsMap) => resultsMap.keys
          }.distinct
        } yield {
          val workspaceExpressionResultsPerEntity = populateWorkspaceLookupPerEntity(workspaceExpressionResults, rootEntities)
          val groupedResults = groupResultsByExpressionAndEntityName(bqExpressionResults ++ workspaceExpressionResultsPerEntity)

          val entityNameAndInputValues = constructInputsForEachEntity(gatherInputsResult, groupedResults, baseTableAlias, rootEntities)

          createSubmissionValidationEntityInputs(CollectionUtils.groupByTuples(entityNameAndInputValues))
        }
        resultIO.unsafeToFuture()

      case _ => Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Only root entity type supported for Data Repo workflows")))
    }
  }

  private def populateWorkspaceLookupPerEntity(workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]], rootEntities: List[EntityName]): List[ExpressionAndResult] = {
    workspaceExpressionResults.toList.map { case(lookup, result) =>
      (lookup, rootEntities.map(_ -> result).toMap)
    }
  }

  private def checkSubmissionSize(parsedExpressions: Set[ParsedDataRepoExpression], tableModel: TableModel) = {
    if (tableModel.getRowCount * parsedExpressions.size > config.maxInputsPerSubmission) {
      IO.raiseError(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"Too many results. Snapshot row count * number of entity expressions cannot exceed ${config.maxInputsPerSubmission}.")))
    } else {
      IO.unit
    }
  }

  private def constructInputsForEachEntity(gatherInputsResult: GatherInputsResult, groupedResults: Seq[(Transformers.LookupExpression, Map[Transformers.EntityName, Try[scala.Iterable[AttributeValue]]])], baseTableAlias: LookupExpression, rootEntities: List[EntityName]): Seq[(EntityName, SubmissionValidationValue)] = {
    // gatherInputsResult.processableInputs.toSeq so that the result is not a Set and does not worry about duplicates
    gatherInputsResult.processableInputs.toSeq.flatMap { input =>
      val parser = AntlrTerraExpressionParser.getParser(input.expression)
      val visitor = new LookupExpressionExtractionVisitor()
      val parsedTree = parser.root()
      val lookupExpressions = visitor.visit(parsedTree)

      val transformers = new Transformers(Option(rootEntities))
      val expressionResultsByEntityName = transformers.transformAndParseExpr(groupedResults.filter {
        case (expression, _) => lookupExpressions.contains(expression)
      }, parsedTree)

      convertToSubmissionValidationValues(expressionResultsByEntityName, input)
    }
  }

  private def groupResultsByExpressionAndEntityName(expressionResults: List[ExpressionAndResult]) = {
    expressionResults.groupBy {
      case (expression, _) => expression
    }.toSeq.map {
      case (expression, groupedList) => (expression, groupedList.foldLeft(Map.empty[EntityName, Try[Iterable[AttributeValue]]]) {
        case (aggregateResults, (_, individualResult)) => aggregateResults ++ individualResult
      })
    }
  }

  private def runBigQueryQueries(entityNameColumn: String, bqQueryJobConfigs: Map[Set[ParsedDataRepoExpression], QueryJobConfiguration], petKey: String): IO[List[ExpressionAndResult]] = {
    bqServiceFactory.getServiceForPet(petKey).use { bqService =>
      for {
        queryResults <- bqQueryJobConfigs.toList.traverse { // should we do parTraverse?
          case (expressions, bqJob) => bqService.query(bqJob).map(expressions -> _)
        }
        _ <- checkQuerySize(queryResults)
      } yield {
        for {
          (parsedExpressions, tableResult) <- queryResults
          resultRow <- tableResult.iterateAll().asScala
          parsedExpression <- parsedExpressions
        } yield {
          val field = tableResult.getSchema.getFields.get(parsedExpression.columnName) // is case sensitivity an issue on columnName?
          val primaryKey: EntityName = resultRow.get(entityNameColumn).getStringValue
          val attribute = fieldToAttribute(field, resultRow)
          val evaluationResult: Try[Iterable[AttributeValue]] = attribute match {
            case v: AttributeValue => Success(Seq(v))
            case AttributeValueList(l) => Success(l)
            case unsupported => Failure(new RawlsException(s"unsupported attribute: $unsupported"))
          }
          (parsedExpression.expression, Map(primaryKey -> evaluationResult))
        }
      }
    }
  }

  private def checkQuerySize(queryResults: List[(Set[ParsedDataRepoExpression], TableResult)]): IO[Unit] = {
    queryResults.traverse { case (queryResult, tableResults) =>
      if (tableResults.getTotalRows > config.maxRowsPerQuery) {
        IO.raiseError(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"Too many results. Results size ${tableResults.getTotalRows} cannot exceed ${config.maxRowsPerQuery}. Expression(s): [${queryResult.map(_.expression).mkString(", ")}].")))
      } else {
        IO.unit
      }
    }.void
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
        val query = s"SELECT $baseTableAlias.$entityNameColumn, ${expressions.map(_.qualifiedColumnName).mkString(", ")} FROM `${dataProject}.${viewName}.${tableModel.getName}` $baseTableAlias"

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

  override def expressionValidator: ExpressionValidator = new DataRepoEntityExpressionValidator(snapshotModel)

}
