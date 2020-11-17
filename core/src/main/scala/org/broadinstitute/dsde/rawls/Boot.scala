package org.broadinstitute.dsde.rawls

import java.io.StringReader
import java.net.InetAddress
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import cats.effect._
import cats.implicits._
import com.codahale.metrics.SharedMetricRegistries
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.json.jackson2.JacksonFactory
import com.readytalk.metrics.{StatsDReporter, WorkbenchStatsD}
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import com.typesafe.scalalogging.LazyLogging
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.HttpDataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.martha.MarthaResolver
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.HttpWorkspaceManagerDAO
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import org.broadinstitute.dsde.rawls.dataaccess.{ExecutionServiceDAO, _}
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityService}
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.google.{HttpGoogleAccessContextManagerDAO, HttpGooglePubSubDAO}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.wdlparsing.{CachingWDLParser, NonCachingWDLParser, WDLParser}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor._
import org.broadinstitute.dsde.rawls.snapshot.SnapshotService
import org.broadinstitute.dsde.rawls.statistics.StatisticsService
import org.broadinstitute.dsde.rawls.status.StatusService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.ScalaConfig._
import org.broadinstitute.dsde.rawls.util._
import org.broadinstitute.dsde.rawls.webservice._
import org.broadinstitute.dsde.rawls.workspace.{WorkspaceService}
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.Json
import org.broadinstitute.dsde.workbench.google.HttpGoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.google2._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util.ExecutionContexts
import org.http4s.Uri
import org.http4s.client.blaze.BlazeClientBuilder

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.{higherKinds, postfixOps}

object Boot extends IOApp with LazyLogging {
  override def run(
                    args: List[String]
                  ): IO[ExitCode] =
    startup() *> ExitCode.Success.pure[IO]

  private def startup(): IO[Unit] = {
    implicit val log4CatsLogger: _root_.io.chrisdavenport.log4cats.Logger[IO] = Slf4jLogger.getLogger[IO]

    // version.conf is generated by sbt
    val conf = ConfigFactory.parseResources("version.conf").withFallback(ConfigFactory.load())
    val gcsConfig = conf.getConfig("gcs")

    // we need an ActorSystem to host our application in
    implicit val system = ActorSystem("rawls")
    implicit val materializer = ActorMaterializer()

    val slickDataSource = DataSource(DatabaseConfig.forConfig[JdbcProfile]("slick", conf))

    val liquibaseConf = conf.getConfig("liquibase")
    val liquibaseChangeLog = liquibaseConf.getString("changelog")
    val initWithLiquibase = liquibaseConf.getBoolean("initWithLiquibase")

    val changelogParams = Map("gcs:appsDomain" -> gcsConfig.getString("appsDomain"))

    if(initWithLiquibase) {
      slickDataSource.initWithLiquibase(liquibaseChangeLog, changelogParams)
    }

    val metricsConf = conf.getConfig("metrics")
    val metricsPrefix = {
      val basePrefix = metricsConf.getString("prefix")
      metricsConf.getBooleanOption("includeHostname") match {
        case Some(true) =>
          val hostname = InetAddress.getLocalHost().getHostName()
          basePrefix + "." + hostname
        case _ => basePrefix
      }
    }

    if (metricsConf.getBooleanOption("enabled").getOrElse(false)) {
      metricsConf.getObjectOption("reporters") match {
        case Some(configObject) =>
          configObject.entrySet.asScala.map(_.toTuple).foreach {
            case ("statsd", conf: ConfigObject) =>
              val statsDConf = conf.toConfig
              startStatsDReporter(
                statsDConf.getString("host"),
                statsDConf.getInt("port"),
                statsDConf.getDuration("period"),
                apiKey = statsDConf.getStringOption("apiKey"))
            case (other, _) =>
              logger.warn(s"Unknown metrics backend: $other")
          }
        case None => logger.info("No metrics reporters defined")
      }
    } else {
      logger.info("Metrics reporting is disabled.")
    }

    val jsonFactory = JacksonFactory.getDefaultInstance
    val clientSecrets = GoogleClientSecrets.load(jsonFactory, new StringReader(gcsConfig.getString("secrets")))
    val clientEmail = gcsConfig.getString("serviceClientEmail")

    val serviceProject = gcsConfig.getString("serviceProject")
    val appName = gcsConfig.getString("appName")
    val pathToPem = gcsConfig.getString("pathToPem")

    //Sanity check deployment manager template path.
    val dmConfig = DeploymentManagerConfig(gcsConfig.getConfig("deploymentManager"))

    val accessContextManagerDAO = new HttpGoogleAccessContextManagerDAO(
      clientEmail,
      pathToPem,
      appName,
      serviceProject,
      workbenchMetricBaseName = metricsPrefix
    )

    initAppDependencies[IO](conf).use { appDependencies =>
      val gcsDAO = new HttpGoogleServicesDAO(
        false,
        clientSecrets,
        clientEmail,
        gcsConfig.getString("subEmail"),
        gcsConfig.getString("pathToPem"),
        gcsConfig.getString("appsDomain"),
        dmConfig.orgID,
        gcsConfig.getString("groupsPrefix"),
        gcsConfig.getString("appName"),
        gcsConfig.getInt("deletedBucketCheckSeconds"),
        serviceProject,
        gcsConfig.getString("tokenEncryptionKey"),
        gcsConfig.getString("tokenSecretsJson"),
        gcsConfig.getString("billingPemEmail"),
        gcsConfig.getString("pathToBillingPem"),
        gcsConfig.getString("billingEmail"),
        gcsConfig.getString("billingGroupEmail"),
        gcsConfig.getStringList("billingGroupEmailAliases").asScala.toList,
        dmConfig.billingProbeEmail,
        gcsConfig.getInt("bucketLogsMaxAge"),
        googleStorageService = appDependencies.googleStorageService,
        googleServiceHttp = appDependencies.googleServiceHttp,
        topicAdmin = appDependencies.topicAdmin,
        workbenchMetricBaseName = metricsPrefix,
        proxyNamePrefix = gcsConfig.getStringOr("proxyNamePrefix", ""),
        deploymentMgrProject = dmConfig.projectID,
        cleanupDeploymentAfterCreating = dmConfig.cleanupDeploymentAfterCreating,
        terraBucketReaderRole = gcsConfig.getString("terraBucketReaderRole"),
        terraBucketWriterRole = gcsConfig.getString("terraBucketWriterRole"),
        accessContextManagerDAO = accessContextManagerDAO
      )


      val pubSubDAO = new HttpGooglePubSubDAO(
        clientEmail,
        pathToPem,
        appName,
        serviceProject,
        workbenchMetricBaseName = metricsPrefix
      )

      // Import service uses a different project for its pubsub topic
      val importServicePubSubDAO = new HttpGooglePubSubDAO(
        clientEmail,
        pathToPem,
        appName,
        conf.getString("avroUpsertMonitor.updateImportStatusPubSubProject"),
        workbenchMetricBaseName = metricsPrefix
      )

      val importServiceDAO = new HttpImportServiceDAO(conf.getString("avroUpsertMonitor.server"))

      val bigQueryDAO = new HttpGoogleBigQueryDAO(
        appName,
        Json(gcsConfig.getString("bigQueryJson")),
        metricsPrefix
      )

      val samConfig = conf.getConfig("sam")
      val samDAO = new HttpSamDAO(
        samConfig.getString("server"),
        gcsDAO.getBucketServiceAccountCredential
      )

      enableServiceAccount(gcsDAO, samDAO)

      system.registerOnTermination {
        slickDataSource.databaseConfig.db.shutdown
      }

      val executionServiceConfig = conf.getConfig("executionservice")
      val submissionTimeout = org.broadinstitute.dsde.rawls.util.toScalaDuration(
        executionServiceConfig.getDuration("workflowSubmissionTimeout")
      )

      val executionServiceServers: Set[ClusterMember] = executionServiceConfig
        .getObject("readServers")
        .entrySet()
        .asScala
        .map { entry =>
          val (strName, strHostname) = entry.toTuple
          ClusterMember(
            ExecutionServiceId(strName),
            new HttpExecutionServiceDAO(
              strHostname.unwrapped.toString,
              metricsPrefix
            )
          )
        }
        .toSet

      val executionServiceSubmitServers: Set[ClusterMember] =
        executionServiceConfig
          .getObject("submitServers")
          .entrySet()
          .asScala
          .map { entry =>
            val (strName, strHostname) = entry.toTuple
            ClusterMember(
              ExecutionServiceId(strName),
              new HttpExecutionServiceDAO(
                strHostname.unwrapped.toString,
                metricsPrefix
              )
            )
          }
          .toSet

      val cromiamDAO: ExecutionServiceDAO = new HttpExecutionServiceDAO(executionServiceConfig.getString("cromiamUrl"), metricsPrefix)
      val shardedExecutionServiceCluster: ExecutionServiceCluster =
        new ShardedHttpExecutionServiceCluster(
          executionServiceServers,
          executionServiceSubmitServers,
          slickDataSource
        )
      val projectOwners =
        gcsConfig.getStringList("projectTemplate.owners").asScala
      val projectEditors =
        gcsConfig.getStringList("projectTemplate.editors").asScala
      val requesterPaysRole = gcsConfig.getString("requesterPaysRole")
      val projectTemplate = ProjectTemplate(projectOwners, projectEditors)

      val notificationDAO = new PubSubNotificationDAO(
        pubSubDAO,
        gcsConfig.getString("notifications.topicName")
      )

      val marthaBaseUrl: String = conf.getString("martha.baseUrl")
      val marthaUrl: String = s"$marthaBaseUrl/martha_v3"
      val marthaResolver = new MarthaResolver(marthaUrl)

      val userServiceConstructor: (UserInfo) => UserService =
        UserService.constructor(
          slickDataSource,
          gcsDAO,
          notificationDAO,
          samDAO,
          requesterPaysRole,
          dmConfig,
          projectTemplate
        )
      val genomicsServiceConstructor: (UserInfo) => GenomicsService =
        GenomicsService.constructor(slickDataSource, gcsDAO)
      val statisticsServiceConstructor: (UserInfo) => StatisticsService =
        StatisticsService.constructor(slickDataSource, gcsDAO)
      val submissionCostService: SubmissionCostService =
        SubmissionCostService.constructor(
          gcsConfig.getString("billingExportTableName"),
          gcsConfig.getString("serviceProject"),
          gcsConfig.getInt("billingSearchWindowDays"),
          bigQueryDAO
        )

      val methodRepoDAO = new HttpMethodRepoDAO(
        MethodRepoConfig.apply[Agora.type](conf.getConfig("agora")),
        MethodRepoConfig.apply[Dockstore.type](conf.getConfig("dockstore")),
        metricsPrefix
      )

      val workspaceManagerDAO = new HttpWorkspaceManagerDAO(conf.getString("workspaceManager.baseUrl"))

      val dataRepoDAO = new HttpDataRepoDAO(conf.getString("dataRepo.terraInstanceName"), conf.getString("dataRepo.terraInstance"))

      val maxActiveWorkflowsTotal =
        conf.getInt("executionservice.maxActiveWorkflowsPerServer")
      val maxActiveWorkflowsPerUser = maxActiveWorkflowsTotal / conf.getInt(
        "executionservice.activeWorkflowHogFactor"
      )
      val useWorkflowCollectionField =
        conf.getBoolean("executionservice.useWorkflowCollectionField")
      val useWorkflowCollectionLabel =
        conf.getBoolean("executionservice.useWorkflowCollectionLabel")
      val defaultBackend: CromwellBackend = CromwellBackend(conf.getString("executionservice.defaultBackend"))

      val wdlParsingConfig = WDLParserConfig(conf.getConfig("wdl-parsing"))
      def cromwellSwaggerClient = new CromwellSwaggerClient(wdlParsingConfig.serverBasePath)

      def wdlParser: WDLParser =
        if (wdlParsingConfig.useCache)
          new CachingWDLParser(wdlParsingConfig, cromwellSwaggerClient)
        else new NonCachingWDLParser(wdlParsingConfig, cromwellSwaggerClient)

      val methodConfigResolver  = new MethodConfigResolver(wdlParser)

      val healthMonitor = system.actorOf(
        HealthMonitor
          .props(
            slickDataSource,
            gcsDAO,
            pubSubDAO,
            methodRepoDAO,
            samDAO,
            executionServiceServers.map(c => c.key -> c.dao).toMap,
            groupsToCheck = Seq(gcsDAO.adminGroupName, gcsDAO.curatorGroupName),
            topicsToCheck = Seq(gcsConfig.getString("notifications.topicName")),
            bucketsToCheck = Seq(gcsDAO.tokenBucketName)
          )
          .withDispatcher("health-monitor-dispatcher"),
        "health-monitor"
      )
      logger.info("Starting health monitor...")
      system.scheduler.schedule(
        10 seconds,
        1 minute,
        healthMonitor,
        HealthMonitor.CheckAll
      )

      val statusServiceConstructor: () => StatusService = () =>
        StatusService.constructor(healthMonitor)

      val workspaceServiceConfig = WorkspaceServiceConfig.apply(conf)

      val bondConfig = conf.getConfig("bond")
      val bondApiDAO: BondApiDAO = new HttpBondApiDAO(bondConfig.getString("baseUrl"))
      val requesterPaysSetupService: RequesterPaysSetupService = new RequesterPaysSetupService(slickDataSource, gcsDAO, bondApiDAO, requesterPaysRole)

      // create the entity manager.
      val entityManager = EntityManager.defaultEntityManager(slickDataSource, workspaceManagerDAO, dataRepoDAO, samDAO, appDependencies.bigQueryServiceFactory, DataRepoEntityProviderConfig(conf.getConfig("dataRepoEntityProvider")))

      val workspaceServiceConstructor: (UserInfo) => WorkspaceService = WorkspaceService.constructor(
        slickDataSource,
        methodRepoDAO,
        cromiamDAO,
        shardedExecutionServiceCluster,
        conf.getInt("executionservice.batchSize"),
        workspaceManagerDAO,
        dataRepoDAO,
        methodConfigResolver,
        gcsDAO,
        samDAO,
        notificationDAO,
        userServiceConstructor,
        genomicsServiceConstructor,
        maxActiveWorkflowsTotal,
        maxActiveWorkflowsPerUser,
        workbenchMetricBaseName = metricsPrefix,
        submissionCostService,
        workspaceServiceConfig,
        requesterPaysSetupService,
        entityManager
      )

      val entityServiceConstructor: (UserInfo) => EntityService = EntityService.constructor(
        slickDataSource,
        samDAO,
        workbenchMetricBaseName = metricsPrefix,
        entityManager
      )

      val snapshotServiceConstructor: (UserInfo) => SnapshotService = SnapshotService.constructor(
        slickDataSource,
        samDAO,
        workspaceManagerDAO,
        gcsDAO.getBucketServiceAccountCredential,
        conf.getString("dataRepo.terraInstanceName")
      )

      val service = new RawlsApiServiceImpl(
        workspaceServiceConstructor,
        entityServiceConstructor,
        userServiceConstructor,
        genomicsServiceConstructor,
        statisticsServiceConstructor,
        snapshotServiceConstructor,
        statusServiceConstructor,
        shardedExecutionServiceCluster,
        ApplicationVersion(
          conf.getString("version.git.hash"),
          conf.getString("version.build.number"),
          conf.getString("version.version")
        ),
        clientSecrets.getDetails.getClientId,
        submissionTimeout,
        metricsPrefix,
        samDAO,
        conf.as[SwaggerConfig]("swagger")
      )

      if (conf.getBooleanOption("backRawls").getOrElse(false)) {
        logger.info("This instance has been marked as BACK. Booting monitors...")

        BootMonitors.bootMonitors(
          system,
          conf,
          slickDataSource,
          gcsDAO,
          samDAO,
          pubSubDAO,
          importServicePubSubDAO,
          importServiceDAO,
          appDependencies.googleStorageService,
          methodRepoDAO,
          marthaResolver,
          entityServiceConstructor,
          shardedExecutionServiceCluster,
          maxActiveWorkflowsTotal,
          maxActiveWorkflowsPerUser,
          userServiceConstructor,
          projectTemplate,
          metricsPrefix,
          requesterPaysRole,
          useWorkflowCollectionField,
          useWorkflowCollectionLabel,
          defaultBackend,
          methodConfigResolver
        )
      } else
        logger.info(
          "This instance has been marked as FRONT. Monitors will not be booted..."
        )

      for {
        binding <- IO.fromFuture(IO(Http().bindAndHandle(service.route, "0.0.0.0", 8080))).recover {
          case t: Throwable =>
            logger.error("FATAL - failure starting http server", t)
            throw t
        }
        _ <- IO.fromFuture(IO(binding.whenTerminated))
        _ <- IO(system.terminate())
      } yield ()
    }
  }

  /**
   * Enables the rawls service account in ldap. Allows service to service auth through the proxy.
   * @param gcsDAO
   */
  def enableServiceAccount(gcsDAO: HttpGoogleServicesDAO, samDAO: HttpSamDAO): Unit = {
    val credential = gcsDAO.getBucketServiceAccountCredential
    val serviceAccountUserInfo = UserInfo.buildFromTokens(credential)

    val registerServiceAccountFuture = samDAO.registerUser(serviceAccountUserInfo)

    registerServiceAccountFuture.failed.foreach {
      // this is logged as a warning because almost always the service account is already enabled
      // so this is a problem only the first time rawls is started with a new service account
      t: Throwable => logger.warn("error enabling service account", t)
    }
  }

  def startStatsDReporter(host: String, port: Int, period: java.time.Duration, registryName: String = "default", apiKey: Option[String] = None): Unit = {
    logger.info(s"Starting statsd reporter writing to [$host:$port] with period [${period.toMillis} ms]")
    val reporter = StatsDReporter.forRegistry(SharedMetricRegistries.getOrCreate(registryName))
      .prefixedWith(apiKey.orNull)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build(WorkbenchStatsD(host, port))
    reporter.start(period.toMillis, period.toMillis, TimeUnit.MILLISECONDS)
  }

  def initAppDependencies[F[_]: ConcurrentEffect: Timer: Logger: ContextShift](config: Config)(implicit executionContext: ExecutionContext): cats.effect.Resource[F, AppDependencies[F]] = {
    val gcsConfig = config.getConfig("gcs")
    val serviceProject = GoogleProject(gcsConfig.getString("serviceProject"))
    val pathToCredentialJson = gcsConfig.getString("pathToCredentialJson")
    val googleApiUri = Uri.unsafeFromString(gcsConfig.getString("google-api-uri"))
    val metadataNotificationConfig = NotificationCreaterConfig(pathToCredentialJson, googleApiUri)

    implicit val logger = Slf4jLogger.getLogger[F]

    for {
      blockingEc <- ExecutionContexts.fixedThreadPool[F](256) //scala.concurrent.blocking has default max extra thread number 256, so use this number to start with
      blocker = Blocker.liftExecutionContext(blockingEc)
      googleStorage <- GoogleStorageService.resource[F](pathToCredentialJson, blocker, None, Option(serviceProject))
      httpClient <- BlazeClientBuilder(executionContext).resource
      googleServiceHttp <- GoogleServiceHttp.withRetryAndLogging(httpClient, metadataNotificationConfig)
      topicAdmin <- GoogleTopicAdmin.fromCredentialPath(pathToCredentialJson)
      bqServiceFactory = new GoogleBigQueryServiceFactory(blocker)(executionContext)
    } yield AppDependencies[F](googleStorage, googleServiceHttp, topicAdmin, bqServiceFactory)
  }
}

// Any resources need clean up should be put in AppDependencies
final case class AppDependencies[F[_]](
  googleStorageService: GoogleStorageService[F],
  googleServiceHttp: GoogleServiceHttp[F],
  topicAdmin: GoogleTopicAdmin[F],
  bigQueryServiceFactory: GoogleBigQueryServiceFactory)
