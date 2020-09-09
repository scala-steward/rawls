package org.broadinstitute.dsde.rawls.dataaccess.martha

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.{ActorMaterializer, Materializer}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

class MarthaDrsResolverSpec extends FlatSpecLike with Matchers with BeforeAndAfterAll {

  implicit val mockActorSystem: ActorSystem = ActorSystem("MockMarthaDosResolver")
  implicit val mockMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val mockExecutionContext: TestExecutionContext = TestExecutionContext.testExecutionContext

  val mockMarthaDrsResolver = new MockMarthaDrsResolver(marthaUrl = "https://martha_v3_url", excludeJDRDomain = true)
  val mockUserInfo = UserInfo(RawlsUserEmail("mr_bean@gmail.com"), OAuth2BearerToken("foo"), 0, RawlsUserSubjectId("abc123"))

  behavior of "Martha DRS resolver"

  it should "return true for correct DEV JDR uri in isJDRDomain()" in {
    MarthaUtils.isJDRDomain("drs://jade.datarepo-dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4") shouldBe true
  }

  it should "return false for look alike JDR uri in isJDRDomain()" in {
    MarthaUtils.isJDRDomain("drs://jade-datarepo.dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4") shouldBe false
  }

  it should "return true for correct PROD JDR uri in isJDRDomain()" in {
    MarthaUtils.isJDRDomain("drs://jade-terra.datarepo-prod.broadinstitute.org/v1_anything") shouldBe true
  }

  it should "return false for non-JDR uri in isJDRDomain()" in {
    MarthaUtils.isJDRDomain("drs://dg.712C/fa640b0e-9779-452f-99a6-16d833d15bd0") shouldBe false
  }

  it should "not explode on URI parse fail" in {
    MarthaUtils.isJDRDomain("zardoz") shouldBe false
  }

  it should "return None for a JDR uri" in {
    val actualResultFuture = mockMarthaDrsResolver.drsServiceAccountEmail(
      drsUrl = MockMarthaDrsResolver.jdrDevUrl,
      userInfo = mockUserInfo
    )

    assertResult(None) {
      Await.result(actualResultFuture, 1 minute)
    }
  }

  it should "return client email for non-JDR uri" in {
    val actualResultFuture = mockMarthaDrsResolver.drsServiceAccountEmail(
      drsUrl = MockMarthaDrsResolver.dgUrl,
      userInfo = mockUserInfo
    )

    assertResult(Option("mr_bean@gmail.com")) {
      Await.result(actualResultFuture, 1 minute)
    }
  }

  /**
    * TODO: Maybe more tests can be added where we mock the response from Martha with
    * MockMarthaDrsResolver(martha_v3 url, excludeJDRDomain = false). This way we can cover the scenario where we do talk
    * to Martha for JDR uris
    */
}

object MockMarthaDrsResolver {

  val jdrDevUrl = "drs://jade.datarepo-dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4"
  val dgUrl = "drs://dg.712C/fa640b0e-9779-452f-99a6-16d833d15bd0"

  val mockEmail = ServiceAccountEmail("mr_bean@gmail.com")
  val mockSAPayload = ServiceAccountPayload(Option(mockEmail))
  val exampleGoogleSA = MarthaMinimalResponse(Option(mockSAPayload))
}


class MockMarthaDrsResolver(marthaUrl: String, excludeJDRDomain: Boolean)
                           (implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext)
  extends MarthaDrsResolver(marthaUrl, excludeJDRDomain) {

  override def getClientEmailFromMartha(drsUrl: String, userInfo: UserInfo): Future[Option[String]] = {
    Future.successful(Option("mr_bean@gmail.com"))
  }
}
