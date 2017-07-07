package org.broadinstitute.dsde.rawls.metrics

import java.util.UUID

import org.scalatest.{FlatSpec, Matchers}
import org.broadinstitute.dsde.rawls.metrics.Expansion._
import org.broadinstitute.dsde.rawls.model.Subsystems.Subsystem
import org.broadinstitute.dsde.rawls.model._
import spray.http._

/**
  * Created by rtitle on 7/16/17.
  */
class ExpansionSpec extends FlatSpec with Matchers {

  "the Expansion typeclass" should "expand WorkspaceNames" in {
    val test = WorkspaceName("test", "workspace")
    assertResult("test.workspace") {
      implicitly[Expansion[WorkspaceName]].makeName(test)
    }
  }

  it should "expand UUIDs" in {
    val test = UUID.randomUUID
    assertResult(test.toString) {
      implicitly[Expansion[UUID]].makeName(test)
    }
  }

  it should "expand RawlsEnumerations" in {
    val subsystem = Subsystems.Database

    // Verify we can summon an implicit for multiple levels in the object hierarchy
    assertResult("Database") {
      implicitly[Expansion[RawlsEnumeration[_]]].makeName(subsystem)
    }
    assertResult("Database") {
      implicitly[Expansion[Subsystem]].makeName(subsystem)
    }
    assertResult("Database") {
      implicitly[Expansion[Subsystems.Database.type]].makeName(subsystem)
    }
  }

  it should "expand HttpMethods" in {
    val test = HttpMethods.POST
    assertResult("post") {
      implicitly[Expansion[HttpMethod]].makeName(test)
    }
  }

  it should "expand URIs" in {
    val test = Uri("http://www.example.com:1234/a/uri/path?q=foo&w=bar")
    assertResult("a.uri.path") {
      implicitly[Expansion[Uri]].makeName(test)
    }
  }

  it should "expand status codes" in {
    val test = StatusCodes.NotFound
    assertResult("404") {
      implicitly[Expansion[StatusCode]].makeName(test)
    }
  }

  it should "expand primitives" in {
    val str = "A String"
    val int = 42
    assertResult(str) {
      implicitly[Expansion[String]].makeName(str)
    }
    assertResult("42") {
      implicitly[Expansion[Int]].makeName(int)
    }
  }
}
