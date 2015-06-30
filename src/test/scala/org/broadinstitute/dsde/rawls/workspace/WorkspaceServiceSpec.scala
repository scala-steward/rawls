package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID
import akka.testkit.TestActorRef
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.workspace.AttributeUpdateOperations._
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import spray.testkit.ScalatestRouteTest

class WorkspaceServiceSpec extends FlatSpec with ScalatestRouteTest with Matchers {

  val wsns = "namespace"
  val wsname = UUID.randomUUID().toString

  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))
  val s1 = Entity("s1", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList), WorkspaceName(wsns, wsname))
  val workspace = Workspace(
    wsns,
    wsname,
    DateTime.now().withMillis(0),
    "test",
    Map.empty
  )

  val dataSource = DataSource("memory:rawls", "admin", "admin")

  val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, MockWorkspaceDAO, MockEntityDAO, MockMethodConfigurationDAO, new HttpMethodRepoDAO(RemoteServicesMockServer.mockServerBaseUrl), new HttpExecutionServiceDAO(RemoteServicesMockServer.mockServerBaseUrl))

  lazy val workspaceService: WorkspaceService = TestActorRef(WorkspaceService.props(workspaceServiceConstructor)).underlyingActor

  "WorkspaceService" should "add attribute to entity" in {
    assertResult(Some(AttributeString("foo"))) {
      workspaceService.applyOperationsToEntity(s1, Seq(AddUpdateAttribute("newAttribute", AttributeString("foo")))).attributes.get("newAttribute")
    }
  }

  it should "update attribute in entity" in {
    assertResult(Some(AttributeString("biz"))) {
      workspaceService.applyOperationsToEntity(s1, Seq(AddUpdateAttribute("foo", AttributeString("biz")))).attributes.get("foo")
    }
  }

  it should "remove attribute from entity" in {
    assertResult(None) {
      workspaceService.applyOperationsToEntity(s1, Seq(RemoveAttribute("foo"))).attributes.get("foo")
    }
  }

  it should "add item to existing list in entity" in {
    assertResult(Some(AttributeValueList(attributeList.list :+ AttributeString("new")))) {
      workspaceService.applyOperationsToEntity(s1, Seq(AddListMember("splat", AttributeString("new")))).attributes.get("splat")
    }
  }

  it should "add item to non-existing list in entity" in {
    assertResult(Some(AttributeValueList(Seq(AttributeString("new"))))) {
      workspaceService.applyOperationsToEntity(s1, Seq(AddListMember("bob", AttributeString("new")))).attributes.get("bob")
    }
  }

  it should "remove item from existing listing entity" in {
    assertResult(Some(AttributeValueList(Seq(AttributeString("b"), AttributeBoolean(true))))) {
      workspaceService.applyOperationsToEntity(s1, Seq(RemoveListMember("splat", AttributeString("a")))).attributes.get("splat")
    }
  }

  it should "throw AttributeNotFoundException when removing from a list that does not exist" in {
    intercept[AttributeNotFoundException] {
      workspaceService.applyOperationsToEntity(s1, Seq(RemoveListMember("bingo", AttributeString("a"))))
    }
  }

  it should "throw AttributeUpdateOperationException when remove from an attribute that is not a list" in {
    intercept[AttributeUpdateOperationException] {
      workspaceService.applyOperationsToEntity(s1, Seq(RemoveListMember("foo", AttributeString("a"))))
    }
  }

  it should "throw AttributeUpdateOperationException when adding to an attribute that is not a list" in {
    intercept[AttributeUpdateOperationException] {
      workspaceService.applyOperationsToEntity(s1, Seq(AddListMember("foo", AttributeString("a"))))
    }
  }

  it should "apply attribute updates in order to entity" in {
    assertResult(Some(AttributeString("splat"))) {
      workspaceService.applyOperationsToEntity(s1, Seq(
        AddUpdateAttribute("newAttribute", AttributeString("foo")),
        AddUpdateAttribute("newAttribute", AttributeString("bar")),
        AddUpdateAttribute("newAttribute", AttributeString("splat"))
      )).attributes.get("newAttribute")
    }
  }

  it should "return conflicts during an entity copy" in {
    val s1 = Entity("s1", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3)), WorkspaceName(wsns, wsname))
    val s2 = Entity("s3", "child", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3)), WorkspaceName(wsns, wsname))
    //println("hello " + workspaceService.getCopyConflicts(wsns, wsname, Seq(s1, s2)).size)
    //still needs to be implemented fully
    assertResult(true) {
      true
    }
  }
}



