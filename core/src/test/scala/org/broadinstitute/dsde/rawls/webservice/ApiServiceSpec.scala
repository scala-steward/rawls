package org.broadinstitute.dsde.rawls.webservice

import java.util.concurrent.TimeUnit
import akka.actor.PoisonPill
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import akka.http.scaladsl.server._
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.stream.ActorMaterializer
import akka.testkit.TestKitBase
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAOImpl
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.coordination.UncoordinatedDataSourceAccess
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.martha.MarthaResolver
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponentWithFlatSpecAndMatchers
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityService}
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.jobexec.{SubmissionMonitorConfig, SubmissionSupervisor}
import org.broadinstitute.dsde.rawls.metrics.{InstrumentationDirectives, RawlsInstrumented, RawlsStatsDTestUtils}
import org.broadinstitute.dsde.rawls.mock._
import org.broadinstitute.dsde.rawls.model.{Agora, ApplicationVersion, Dockstore, RawlsBillingAccountName, RawlsUser}
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.snapshot.SnapshotService
import org.broadinstitute.dsde.rawls.spendreporting.SpendReportingService
import org.broadinstitute.dsde.rawls.status.StatusService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.workspace.{MultiCloudWorkspaceService, WorkspaceService}
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleBigQueryDAO, MockGoogleIamDAO}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.concurrent.Eventually
import spray.json._

import scala.concurrent.duration._
import scala.language.postfixOps

//noinspection TypeAnnotation
// common trait to be inherited by API service tests
trait ApiServiceSpec extends TestDriverComponentWithFlatSpecAndMatchers with RawlsTestUtils with RawlsInstrumented
  with RawlsStatsDTestUtils with InstrumentationDirectives with ScalatestRouteTest with TestKitBase
  with SprayJsonSupport with MockitoTestUtils with Eventually with LazyLogging {

  // increase the timeout for ScalatestRouteTest from the default of 1 second, otherwise
  // intermittent failures occur on requests not completing in time
  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  // this gets fed into sealRoute so that exceptions are handled the same in tests as in real life
  implicit val exceptionHandler = RawlsApiService.exceptionHandler

  override val workbenchMetricBaseName = "test"

  def actorRefFactory = system

  val mockServer = RemoteServicesMockServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer()
  }

  override def afterAll(): Unit = {
    mockServer.stopServer
    super.afterAll()
  }

  def httpJsonStr(str: String) = HttpEntity(ContentTypes.`application/json`, str)
  def httpJson[T](obj: T)(implicit writer: JsonWriter[T]) = httpJsonStr(obj.toJson.toString())
  val httpJsonEmpty = httpJsonStr("[]")

  def revokeCuratorRole(services: ApiServices, user: RawlsUser = testData.userOwner): Unit = {
    Get("/user/role/curator") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    Delete(s"/admin/user/role/curator/${user.userEmail.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    Get("/user/role/curator") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  //noinspection TypeAnnotation,NameBooleanParameters,ConvertibleToMethodValue,UnitMethodIsParameterless
  trait ApiServices extends AdminApiService with BillingApiService with BillingApiServiceV2 with EntityApiService
    with NotificationsApiService with RawlsApiService with SnapshotApiService with StatusApiService with UserApiService with WorkspaceApiService {

    val dataSource: SlickDataSource
    val gcsDAO: MockGoogleServicesDAO
    val gpsDAO: MockGooglePubSubDAO

    def actorRefFactory = system

    override implicit val materializer = ActorMaterializer()
    override val workbenchMetricBaseName: String = "test"
    override val swaggerConfig: SwaggerConfig = SwaggerConfig("foo", "bar")
    override val submissionTimeout = FiniteDuration(1, TimeUnit.MINUTES)

    val samDAO: SamDAO = new MockSamDAO(dataSource)

    val workspaceManagerDAO: WorkspaceManagerDAO = new MockWorkspaceManagerDAO()

    val dataRepoDAO: DataRepoDAO = new MockDataRepoDAO(mockServer.mockServerBaseUrl)

    val bigQueryServiceFactory: GoogleBigQueryServiceFactory = MockBigQueryServiceFactory.ioFactory()

    override val executionServiceCluster = MockShardedExecutionServiceCluster.fromDAO(new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName), slickDataSource)

    val config = SubmissionMonitorConfig(5 seconds, true, 20000)
    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      executionServiceCluster,
      new UncoordinatedDataSourceAccess(slickDataSource),
      samDAO,
      gcsDAO,
      gcsDAO.getBucketServiceAccountCredential,
      config,
      workbenchMetricBaseName
    ).withDispatcher("submission-monitor-dispatcher"))

    val testConf = ConfigFactory.load()

    override val batchUpsertMaxBytes = testConf.getLong("entityUpsert.maxContentSizeBytes")

    val googleGroupSyncTopic = "test-topic-name"

    val notificationTopic = "test-notification-topic"
    val notificationDAO = new PubSubNotificationDAO(gpsDAO, notificationTopic)

    val drsResolver = new MarthaResolver(mockServer.mockServerBaseUrl)

    val servicePerimeterConfig = ServicePerimeterServiceConfig(testConf)
    val servicePerimeterService = new ServicePerimeterService(slickDataSource, gcsDAO, servicePerimeterConfig)

    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      samDAO,
      new MultiCloudWorkspaceConfig(false, None, None)
    )
    override val userServiceConstructor = UserService.constructor(
      slickDataSource,
      gcsDAO,
      notificationDAO,
      samDAO,
      MockBigQueryServiceFactory.ioFactory(),
      testConf.getString("gcs.pathToCredentialJson"),
      "requesterPaysRole",
      DeploymentManagerConfig(testConf.getConfig("gcs.deploymentManager")),
      ProjectTemplate.from(testConf.getConfig("gcs.projectTemplate")),
      servicePerimeterService,
      RawlsBillingAccountName("billingAccounts/ABCDE-FGHIJ-KLMNO"),
      billingProfileManagerDAO
    )_

    override val snapshotServiceConstructor = SnapshotService.constructor(
      slickDataSource,
      samDAO,
      workspaceManagerDAO,
      mockServer.mockServerBaseUrl
    )

    override val genomicsServiceConstructor = GenomicsService.constructor(
      slickDataSource,
      gcsDAO
    )_

    val spendReportingBigQueryService = bigQueryServiceFactory.getServiceFromJson("json", GoogleProject("test-project"))
    val spendReportingServiceConfig = SpendReportingServiceConfig("test", 90)
    override val spendReportingConstructor = SpendReportingService.constructor(
      slickDataSource,
      spendReportingBigQueryService,
      samDAO,
      spendReportingServiceConfig
    )

    val methodRepoDAO = new HttpMethodRepoDAO(
      MethodRepoConfig[Agora.type](mockServer.mockServerBaseUrl, ""),
      MethodRepoConfig[Dockstore.type](mockServer.mockServerBaseUrl, ""),
      workbenchMetricBaseName = workbenchMetricBaseName)

    val healthMonitor = system.actorOf(HealthMonitor.props(
      dataSource, gcsDAO, gpsDAO, methodRepoDAO, samDAO, executionServiceCluster.readMembers.map(c => c.key->c.dao).toMap,
      Seq("my-favorite-group"), Seq.empty, Seq("my-favorite-bucket")))
    override val statusServiceConstructor = StatusService.constructor(healthMonitor)_
    val bigQueryDAO = new MockGoogleBigQueryDAO
    val submissionCostService = new MockSubmissionCostService("test", "test", 31, bigQueryDAO)
    val execServiceBatchSize = 3
    val maxActiveWorkflowsTotal = 10
    val maxActiveWorkflowsPerUser = 2
    val workspaceServiceConfig = WorkspaceServiceConfig(
      true,
      "fc-"
    )

    val bondApiDAO: BondApiDAO = new MockBondApiDAO(bondBaseUrl = "bondUrl")
    val requesterPaysSetupService = new RequesterPaysSetupService(slickDataSource, gcsDAO, bondApiDAO, requesterPaysRole = "requesterPaysRole")

    val entityManager = EntityManager.defaultEntityManager(dataSource, workspaceManagerDAO, dataRepoDAO, samDAO, bigQueryServiceFactory, DataRepoEntityProviderConfig(100, 10, 0), testConf.getBoolean("entityStatisticsCache.enabled"))

    val resourceBufferDAO: ResourceBufferDAO = new MockResourceBufferDAO
    val resourceBufferConfig = ResourceBufferConfig(testConf.getConfig("resourceBuffer"))
    val resourceBufferService = new ResourceBufferService(resourceBufferDAO, resourceBufferConfig)
    val resourceBufferSaEmail = resourceBufferConfig.saEmail

    override val workspaceServiceConstructor = WorkspaceService.constructor(
      slickDataSource,
      methodRepoDAO,
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName),
      executionServiceCluster,
      execServiceBatchSize,
      workspaceManagerDAO,
      methodConfigResolver,
      gcsDAO,
      samDAO,
      notificationDAO,
      userServiceConstructor,
      genomicsServiceConstructor,
      maxActiveWorkflowsTotal,
      maxActiveWorkflowsPerUser,
      workbenchMetricBaseName,
      submissionCostService,
      workspaceServiceConfig,
      requesterPaysSetupService,
      entityManager,
      resourceBufferService,
      resourceBufferSaEmail,
      servicePerimeterService,
      googleIamDao = new MockGoogleIamDAO,
      terraBillingProjectOwnerRole = "fakeTerraBillingProjectOwnerRole",
      terraWorkspaceCanComputeRole = "fakeTerraWorkspaceCanComputeRole"
    )_

    override val multiCloudWorkspaceServiceConstructor = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      samDAO,
      MultiCloudWorkspaceConfig(testConf)
    )

    override val entityServiceConstructor = EntityService.constructor(
      slickDataSource,
      samDAO,
      workbenchMetricBaseName,
      entityManager,
     1000
    )_

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
    }

    val appVersion = ApplicationVersion("dummy", "dummy", "dummy")
    val googleClientId = "dummy"

    // for metrics testing
    val sealedInstrumentedRoutes: Route = instrumentRequest {
      sealRoute(adminRoutes ~ billingRoutesV2 ~ billingRoutes ~ entityRoutes ~ methodConfigRoutes ~ notificationsRoutes ~ statusRoute ~
        submissionRoutes ~ userRoutes ~ workspaceRoutes)
    }
  }

}
