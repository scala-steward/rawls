package org.broadinstitute.dsde.rawls.integrationtest

import java.util.UUID
import akka.actor.ActorSystem
import org.joda.time.DateTime

import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration
import scala.util.Try
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import org.broadinstitute.dsde.rawls.dataaccess.{DataSource, GraphAuthDAO, Retry, HttpGoogleServicesDAO}
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import spray.http.OAuth2BearerToken

import org.broadinstitute.dsde.rawls.TestExecutionContext.testExecutionContext

class HttpGoogleServicesDAOSpec extends FlatSpec with Matchers with IntegrationTestConfig with BeforeAndAfterAll with Retry {
  implicit val system = ActorSystem("HttpGoogleCloudStorageDAOSpec")
  val gcsDAO = new HttpGoogleServicesDAO(
    true, // use service account to manage buckets
    gcsConfig.getString("secrets"),
    gcsConfig.getString("pathToPem"),
    gcsConfig.getString("appsDomain"),
    gcsConfig.getString("groupsPrefix"),
    gcsConfig.getString("appName"),
    gcsConfig.getInt("deletedBucketCheckSeconds"),
    gcsConfig.getString("serviceProject"),
    gcsConfig.getString("tokenEncryptionKey"),
    gcsConfig.getString("tokenSecretsJson")
  )

  val testProject = "broad-dsde-dev"
  val testWorkspaceId = UUID.randomUUID.toString
  val testBucket = gcsDAO.getBucketName(testWorkspaceId)
  val testWorkspace = WorkspaceName(testProject, "someName")

  val testCreator = UserInfo(gcsDAO.clientSecrets.getDetails.get("client_email").toString, OAuth2BearerToken("testtoken"), 123, "123456789876543212345")
  val testCollaborator = UserInfo("fake_user_42@broadinstitute.org", OAuth2BearerToken("testtoken"), 123, "123456789876543212345aaa")

  override def afterAll() = {
    Try(gcsDAO.deleteBucket(testCreator,testWorkspaceId)) // one last-gasp attempt at cleaning up
  }

  "HttpGoogleServicesDAO" should "do all of the things" in {
    Await.result(gcsDAO.setupWorkspace(testCreator, testProject, testWorkspaceId, testWorkspace), Duration.Inf)

    val storage = gcsDAO.getStorage(gcsDAO.getBucketServiceAccountCredential)

    // if this does not throw an exception, then the bucket exists
    val bucketResource = Await.result(retry(when500)(() => Future { storage.buckets.get(testBucket).execute() }), Duration.Inf)

    val readerGroup = gcsDAO.toGroupId(testBucket, WorkspaceAccessLevels.Read)
    val writerGroup = gcsDAO.toGroupId(testBucket, WorkspaceAccessLevels.Write)
    val ownerGroup = gcsDAO.toGroupId(testBucket, WorkspaceAccessLevels.Owner)

    // check that the access level for each group is what we expect
    val readerBAC = Await.result(retry(when500)(() => Future { storage.bucketAccessControls.get(testBucket, gcsDAO.makeGroupEntityString(readerGroup)).execute() }), Duration.Inf)
    val writerBAC = Await.result(retry(when500)(() => Future { storage.bucketAccessControls.get(testBucket, gcsDAO.makeGroupEntityString(writerGroup)).execute() }), Duration.Inf)
    val ownerBAC = Await.result(retry(when500)(() => Future { storage.bucketAccessControls.get(testBucket, gcsDAO.makeGroupEntityString(ownerGroup)).execute() }), Duration.Inf)
    val svcAcctBAC = Await.result(retry(when500)(() => Future { storage.bucketAccessControls.get(testBucket, "user-" + gcsDAO.serviceAccountClientId).execute() }), Duration.Inf)

    readerBAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    writerBAC.getRole should be (WorkspaceAccessLevels.Write.toString)
    ownerBAC.getRole should be (WorkspaceAccessLevels.Write.toString)
    svcAcctBAC.getRole should be (WorkspaceAccessLevels.Owner.toString)

    // check that the access level for each group is what we expect
    val readerDOAC = Await.result(retry(when500)(() => Future { storage.defaultObjectAccessControls.get(testBucket, gcsDAO.makeGroupEntityString(readerGroup)).execute() }), Duration.Inf)
    val writerDOAC = Await.result(retry(when500)(() => Future { storage.defaultObjectAccessControls.get(testBucket, gcsDAO.makeGroupEntityString(writerGroup)).execute() }), Duration.Inf)
    val ownerDOAC = Await.result(retry(when500)(() => Future { storage.defaultObjectAccessControls.get(testBucket, gcsDAO.makeGroupEntityString(ownerGroup)).execute() }), Duration.Inf)
    val svcAcctDOAC = Await.result(retry(when500)(() => Future { storage.defaultObjectAccessControls.get(testBucket, "user-" + gcsDAO.serviceAccountClientId).execute() }), Duration.Inf)

    readerDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    writerDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    ownerDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    svcAcctDOAC.getRole should be (WorkspaceAccessLevels.Owner.toString)

    // check that the groups exist (i.e. that this doesn't throw exceptions)
    val directory = gcsDAO.getGroupDirectory
    val readerResource = Await.result(retry(when500)(() => Future { directory.groups.get(readerGroup).execute() }), Duration.Inf)
    val writerResource = Await.result(retry(when500)(() => Future { directory.groups.get(writerGroup).execute() }), Duration.Inf)
    val ownerResource = Await.result(retry(when500)(() => Future { directory.groups.get(ownerGroup).execute() }), Duration.Inf)

    // check that the creator is an owner, and that getACL is consistent
    Await.result(gcsDAO.getMaximumAccessLevel(gcsDAO.toProxyFromUser(RawlsUser(testCreator)), testWorkspaceId), Duration.Inf) should be (WorkspaceAccessLevels.Owner)
    Await.result(gcsDAO.getACL(testWorkspaceId), Duration.Inf).acl should be (Map(gcsDAO.toProxyFromUser(RawlsUser(testCreator)) -> WorkspaceAccessLevels.Owner))

    // try adding a user, changing their access, then revoking it
    Await.result(gcsDAO.updateACL(testCreator, testWorkspaceId, Map(Left(RawlsUser(testCollaborator)) -> WorkspaceAccessLevels.Read)), Duration.Inf)
    Await.result(gcsDAO.getMaximumAccessLevel(gcsDAO.toProxyFromUser(RawlsUser(testCollaborator)), testWorkspaceId), Duration.Inf) should be (WorkspaceAccessLevels.Read)
    Await.result(gcsDAO.updateACL(testCreator, testWorkspaceId, Map(Left(RawlsUser(testCollaborator)) -> WorkspaceAccessLevels.Write)), Duration.Inf)
    Await.result(gcsDAO.getMaximumAccessLevel(gcsDAO.toProxyFromUser(RawlsUser(testCollaborator)), testWorkspaceId), Duration.Inf) should be (WorkspaceAccessLevels.Write)
    Await.result(gcsDAO.updateACL(testCreator, testWorkspaceId, Map(Left(RawlsUser(testCollaborator)) -> WorkspaceAccessLevels.NoAccess)), Duration.Inf)
    Await.result(gcsDAO.getMaximumAccessLevel(gcsDAO.toProxyFromUser(RawlsUser(testCollaborator)), testWorkspaceId), Duration.Inf) should be (WorkspaceAccessLevels.NoAccess)

    // check that we can properly deconstruct group names
    val groupName = gcsDAO.toGroupId(testBucket, WorkspaceAccessLevels.Owner)
    gcsDAO.fromGroupId(groupName) should be (Some(WorkspacePermissionsPair(testWorkspaceId, WorkspaceAccessLevels.Owner)))

    // delete the bucket. confirm that the corresponding groups are deleted
    Await.result(gcsDAO.deleteBucket(testCreator, testWorkspaceId), Duration.Inf)
    intercept[GoogleJsonResponseException] { directory.groups.get(readerGroup).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(writerGroup).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(ownerGroup).execute() }
  }

  it should "crud tokens" in {
    val userInfo = UserInfo(null, null, 0, UUID.randomUUID().toString)
    assertResult(None) { Await.result(gcsDAO.getToken(RawlsUser(userInfo)), Duration.Inf) }
    assertResult(None) { Await.result(gcsDAO.getTokenDate(userInfo), Duration.Inf) }
    Await.result(gcsDAO.storeToken(userInfo, "testtoken"), Duration.Inf)
    assertResult(Some("testtoken")) { Await.result(gcsDAO.getToken(RawlsUser(userInfo)), Duration.Inf) }
    val storeTime = Await.result(gcsDAO.getTokenDate(userInfo), Duration.Inf).get

    Thread.sleep(100)

    Await.result(gcsDAO.storeToken(userInfo, "testtoken2"), Duration.Inf)
    assertResult(Some("testtoken2")) { Await.result(gcsDAO.getToken(RawlsUser(userInfo)), Duration.Inf) }
    assert(Await.result(gcsDAO.getTokenDate(userInfo), Duration.Inf).get.isAfter(storeTime))

    Await.result(gcsDAO.deleteToken(userInfo), Duration.Inf)
    assertResult(None) { Await.result(gcsDAO.getToken(RawlsUser(userInfo)), Duration.Inf) }
    assertResult(None) { Await.result(gcsDAO.getTokenDate(userInfo), Duration.Inf) }
  }

  it should "crud proxy groups" in {
    val user = RawlsUser(UserInfo("foo@bar.com", null, 0, UUID.randomUUID().toString))
    Await.result(gcsDAO.createProxyGroup(user), Duration.Inf)
    assert(! Await.result(gcsDAO.isUserInProxyGroup(user), Duration.Inf))
    Await.result(gcsDAO.addUserToProxyGroup(user), Duration.Inf)
    assert(Await.result(gcsDAO.isUserInProxyGroup(user), Duration.Inf))
    Await.result(gcsDAO.removeUserFromProxyGroup(user), Duration.Inf)
    assert(! Await.result(gcsDAO.isUserInProxyGroup(user), Duration.Inf))

    gcsDAO.getGroupDirectory.groups().delete(gcsDAO.toProxyFromUser(user.userSubjectId)).execute()
  }

  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case gjre: GoogleJsonResponseException => gjre.getStatusCode/100 == 5
      case _ => false
    }
  }

  it should "get test access tokens" in {
    val testSubjectIds = Seq(
      "101003016572470361594",
      "107264531918890164218",
      "116828392799326776060",
      "107581940226251553551",
      "107702194464453345813",
      "100395083369225981496",
      "118157073958495508773",
      "102608047191895289996",
      "106856784632227419965",
      "105641999439366958427",
      "108337488348165973252",
      "102706073583125964500",
      "116003888484393050223",
      "112879436606843526084",
      "114013986610521205819",
      "107400565643514866017",
      "106444922929061577605",
      "105559470680707689945",
      "115476624657025915132",
      "110049696258854645771",
      "108808117718557305030",
      "103258289993704149469",
      "100041954772739354410",
      "108423263562337293300",
      "115612666055810599938",
      "100990973039179724001",
      "118335201734659186543",
      "108017723928723184826",
      "115518025212902613214",
      "111531624426060032620",
      "111913404562495918819",
      "108826610992898357382",
      "106817634567620894561",
      "100991351742623929168",
      "106074493670775102497",
      "101291426649495841464",
      "108535693285237487487",
      "110738775773417828518",
      "114012736439192304805",
      "109033120827903999079",
      "113604106424576781615",
      "115537825650382659581",
      "106561806034285019583",
      "116665660796180358996",
      "111316602870538636519",
      "115212203199039855202",
      "115176743111382416656",
      "100136844457952938288",
      "118413449185735084350",
      "107468954841489752952")

    val accessTokens = testSubjectIds.map { sub =>
      (sub, gcsDAO.getUserCredentials(RawlsUserRef(RawlsUserSubjectId(sub))) map { credOpt =>
        credOpt.map { cred => cred.refreshToken(); cred.getAccessToken }
      })
    }

    val authDAO = new GraphAuthDAO
    val dataSource = DataSource("plocal:/Users/dvoet/projects/rawls/rawlsdb", orientRootUser, orientRootPassword, 0, 30)
    println("subjectId,accessToken,project")
    accessTokens.foreach { case (subjectId, accessTokenF) =>
      val email = dataSource.inTransaction()(txn => authDAO.loadUser(RawlsUserRef(RawlsUserSubjectId(subjectId)), txn).get.userEmail.value.stripSuffix("@firecloud.org"))
      val accessToken = Await.result(accessTokenF, Duration.Inf).getOrElse("")
      println(s"$subjectId,$accessToken,$email")
    }
  }
}