package org.broadinstitute.dsde.rawls.model

import java.util.UUID

import bio.terra.workspace.model.CloningInstructionsEnum.NOTHING
import bio.terra.workspace.model.ReferenceTypeEnum.DATA_REPO_SNAPSHOT
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import spray.json._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class DataReferenceModelSpec extends AnyFreeSpec with Matchers {

  "DataReferenceModel" - {
    "stringOrNull() does the right thing" in {
      assertResult(JsNull) {
        stringOrNull(null)
      }
      assertResult(JsString("x")) {
        stringOrNull("x")
      }
      assertResult(JsString(DATA_REPO_SNAPSHOT.toString)) {
        stringOrNull(DATA_REPO_SNAPSHOT)
      }
    }

    "V2 model to V1 model conversion works" in {
      val referenceId = UUID.randomUUID()
      val workspaceId = UUID.randomUUID()

      assertResult {
        new DataReferenceList().resources(ArrayBuffer(
          new DataReferenceDescription()
            .referenceId(referenceId)
            .name("test-ref")
            .description("test description")
            .workspaceId(workspaceId)
            .referenceType(DATA_REPO_SNAPSHOT)
            .reference(new DataRepoSnapshot().instanceName("test-instance").snapshot("test-snapshot"))
            .cloningInstructions(NOTHING)
        ).asJava)
      } {
        resourceListToReferenceList(
          new ResourceList().resources(ArrayBuffer(
            new ResourceDescription()
              .metadata(new ResourceMetadata()
                .name("test-ref")
                .description("test description")
                .workspaceId(workspaceId)
                .cloningInstructions(NOTHING)
                .resourceId(referenceId)
              )
              .resourceAttributes(new ResourceAttributesUnion()
                .gcpDataRepoSnapshot(new DataRepoSnapshotAttributes().instanceName("test-instance").snapshot("test-snapshot"))
              )
          ).asJava)
        )
      }
    }

    "JSON logic" - {

      "ResourceList, which contains ResourceDescription, which contains DataRepoSnapshotResource" in {
        val resourceId = UUID.randomUUID()
        val workspaceId = UUID.randomUUID()
        assertResult {
          s"""{"resources":[{"metadata":{"workspaceId":"$workspaceId","resourceId":"$resourceId","name":"test-ref","description":"test description","resourceType":"${ResourceType.DATA_REPO_SNAPSHOT}","stewardshipType":"${StewardshipType.REFERENCED}","cloudPlatform":"${CloudPlatform.GCP}","cloningInstructions":"${CloningInstructionsEnum.NOTHING}"},"resourceAttributes":{"gcpDataRepoSnapshot":{"instanceName":"test-instance","snapshot":"test-snapshot"}}}]}""".parseJson
        } {
          new ResourceList().resources(ArrayBuffer(
            new ResourceDescription()
              .metadata(new ResourceMetadata()
                .workspaceId(workspaceId)
                .resourceId(resourceId)
                .name("test-ref")
                .description("test description")
                .resourceType(ResourceType.DATA_REPO_SNAPSHOT)
                .stewardshipType(StewardshipType.REFERENCED)
                .cloudPlatform(CloudPlatform.GCP)
                .cloningInstructions(CloningInstructionsEnum.NOTHING)
              )
              .resourceAttributes(new ResourceAttributesUnion()
                .gcpDataRepoSnapshot(new DataRepoSnapshotAttributes().instanceName("test-instance").snapshot("test-snapshot")))
          ).asJava).toJson
        }
      }

      "DataRepoSnapshotResource with bad UUID's should fail" in {
        assertThrows[DeserializationException] {
          s"""{"metadata":{"workspaceId":"abcd","resourceId":"abcd","name":"test-ref","description":"test description","resourceType":"${ResourceType.DATA_REPO_SNAPSHOT}","stewardshipType":"${StewardshipType.REFERENCED}","cloudPlatform":"${CloudPlatform.GCP}","cloningInstructions":"${CloningInstructionsEnum.NOTHING}"},"attributes":{"instanceName":"test-instance","snapshot":"test-snapshot"}}""".parseJson.convertTo[DataRepoSnapshotResource]
        }
      }

      "UpdateDataReferenceRequestBody should work when updating name and description" in {
        assertResult {
          s"""{"name":"foo","description":"bar"}""".parseJson
        } {
          new UpdateDataReferenceRequestBody().name("foo").description("bar").toJson
        }
      }

      "UpdateDataReferenceRequestBody should work with only one parameter" in {
        assertResult {
          s"""{"name":null,"description":"foo"}""".parseJson
        } {
          new UpdateDataReferenceRequestBody().description("foo").toJson
        }
      }

      "UpdateDataReferenceRequestBody with no parameters should fail" in {
        assertThrows[DeserializationException] {
          s"""{}""".parseJson.convertTo[UpdateDataReferenceRequestBody]
        }
      }
    }
  }
}
