package org.broadinstitute.dsde.rawls.mock

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{DataReferenceDescriptionField, DataReferenceName, ErrorReport}

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap

class MockWorkspaceManagerDAO extends WorkspaceManagerDAO {

  val references: TrieMap[(UUID, UUID), DataRepoSnapshotResource] = TrieMap()

  def mockGetWorkspaceResponse(workspaceId: UUID) = new WorkspaceDescription().id(workspaceId)
  def mockCreateWorkspaceResponse(workspaceId: UUID) = new CreatedWorkspace().id(workspaceId)
  def mockReferenceResponse(workspaceId: UUID, referenceId: UUID) = references.getOrElse((workspaceId, referenceId), throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found")))
  def mockEnumerateReferenceResponse(workspaceId: UUID) = new ResourceList().resources(references.collect {
    case ((wsId, _), refDescription) if wsId == workspaceId => new ResourceDescription().metadata(refDescription.getMetadata).resourceAttributes(new ResourceAttributesUnion().gcpDataRepoSnapshot(refDescription.getAttributes))
  }.toList.asJava)

  override def getWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): WorkspaceDescription = mockGetWorkspaceResponse(workspaceId)

  override def createWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): CreatedWorkspace = mockCreateWorkspaceResponse(workspaceId)

  override def deleteWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): Unit = ()

  override def createDataRepoSnapshotReference(workspaceId: UUID, name: DataReferenceName, description: Option[DataReferenceDescriptionField], referenceType: ReferenceTypeEnum, reference: DataRepoSnapshotAttributes, cloningInstructions: CloningInstructionsEnum, accessToken: OAuth2BearerToken): DataRepoSnapshotResource = {
    if(reference.toString.contains("fakesnapshot"))
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found"))
    else {
      val newId = UUID.randomUUID()
      val snapshotMetadata = new ResourceMetadata().name(name.value).workspaceId(workspaceId).cloningInstructions(CloningInstructionsEnum.NOTHING)
      description.map(d => snapshotMetadata.description(d.value))
      val ref = new DataRepoSnapshotResource().metadata(snapshotMetadata).attributes(new DataRepoSnapshotAttributes().snapshot(newId.toString))
      references.put((workspaceId, newId), ref)
      mockReferenceResponse(workspaceId, newId)
    }
  }

  override def getDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): DataRepoSnapshotResource = {
    mockReferenceResponse(workspaceId, referenceId)
  }

  override def getDataRepoSnapshotReferenceByName(workspaceId: UUID, refName: DataReferenceName, accessToken: OAuth2BearerToken): DataRepoSnapshotResource = {
    this.references.find {
      case ((workspaceUUID, _), ref) => workspaceUUID == workspaceId && ref.getMetadata.getName == refName.value
    }.getOrElse(throw new ApiException(StatusCodes.NotFound.intValue, s"$refName not found in workspace $workspaceId"))._2
  }

  override def enumerateDataRepoSnapshotReferences(workspaceId: UUID, offset: Int, limit: Int, accessToken: OAuth2BearerToken): ResourceList = {
    mockEnumerateReferenceResponse(workspaceId)
  }

  override def updateDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, updateInfo: UpdateDataReferenceRequestBody, accessToken: OAuth2BearerToken): Unit = {
    if (references.contains(workspaceId, referenceId)) {
      val existingRef = references.get(workspaceId, referenceId).get
      references.update((workspaceId, referenceId), existingRef.metadata(existingRef.getMetadata.name(
        if (updateInfo.getName != null) updateInfo.getName else existingRef.getMetadata.getName
      ).description(
        if (updateInfo.getDescription != null) updateInfo.getDescription else existingRef.getMetadata.getDescription
      )))
    }
  }

  override def deleteDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): Unit = {
    if (references.contains(workspaceId, referenceId))
      references -= ((workspaceId, referenceId))
  }

  override def createBigQueryDatasetReference(workspaceId: UUID, metadata: ReferenceResourceCommonFields, dataset: GcpBigQueryDatasetAttributes, accessToken: OAuth2BearerToken): GcpBigQueryDatasetResource = new GcpBigQueryDatasetResource

  override def deleteBigQueryDatasetReference(workspaceId: UUID, resourceId: UUID, accessToken: OAuth2BearerToken): Unit = ()

  override def getBigQueryDatasetReferenceByName(workspaceId: UUID, name: String, accessToken: OAuth2BearerToken): GcpBigQueryDatasetResource = new GcpBigQueryDatasetResource().metadata(new ResourceMetadata().resourceId(UUID.randomUUID())).attributes(new GcpBigQueryDatasetAttributes())
}
