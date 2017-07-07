package org.broadinstitute.dsde.rawls.metrics

import com.google.api.services.admin.directory.model.{Group, Member, Members}
import com.google.api.services.cloudbilling.model.{BillingAccount, ListBillingAccountsResponse, ProjectBillingInfo}
import com.google.api.services.genomics.model.{ListOperationsResponse, Operation}
import com.google.api.services.pubsub.model.{Empty => PubSubEmpty, _}
import com.google.api.services.storage.model._
import org.broadinstitute.dsde.rawls.model.Subsystems
import org.broadinstitute.dsde.rawls.model.Subsystems.Subsystem

/**
  * Typeclass which maps a Google request type (e.g. Topic, Bucket, etc) to a specific values of the
  * Rawls [[Subsystem]] enumeration.
  */
sealed trait GoogleSubsystemMapper[A] {
  def subsystem: Subsystem
}

object GoogleSubsystemMapper {
  def apply[A](sys: Subsystem): GoogleSubsystemMapper[A] = new GoogleSubsystemMapper[A] {
    override def subsystem: Subsystem = sys
  }

  // GoogleBuckets

  implicit object BucketMapper extends GoogleSubsystemMapper[Bucket] {
    override def subsystem: Subsystem = Subsystems.GoogleBuckets
  }

  implicit object BucketAccessControlMapper extends GoogleSubsystemMapper[BucketAccessControl] {
    override def subsystem: Subsystem = Subsystems.GoogleBuckets
  }

  implicit object BucketAccessControlsMapper extends GoogleSubsystemMapper[BucketAccessControls] {
    override def subsystem: Subsystem = Subsystems.GoogleBuckets
  }

  implicit object StorageObjectControlMapper extends GoogleSubsystemMapper[StorageObject] {
    override def subsystem: Subsystem = Subsystems.GoogleBuckets
  }

  implicit object ObjectsMapper extends GoogleSubsystemMapper[Objects] {
    override def subsystem: Subsystem = Subsystems.GoogleBuckets
  }

  // Google PubSub

  implicit object TopicMapper extends GoogleSubsystemMapper[Topic] {
    override def subsystem: Subsystem = Subsystems.GooglePubSub
  }

  implicit object PubSubEmptyMapper extends GoogleSubsystemMapper[PubSubEmpty] {
    override def subsystem: Subsystem = Subsystems.GooglePubSub
  }

  implicit object SubscriptionMapper extends GoogleSubsystemMapper[Subscription] {
    override def subsystem: Subsystem = Subsystems.GooglePubSub
  }

  implicit object PublishResponseMapper extends GoogleSubsystemMapper[PublishResponse] {
    override def subsystem: Subsystem = Subsystems.GooglePubSub
  }

  implicit object PullResponseMapper extends GoogleSubsystemMapper[PullResponse] {
    override def subsystem: Subsystem = Subsystems.GooglePubSub
  }

  implicit object PolicySubsystem extends GoogleSubsystemMapper[Policy] {
    override def subsystem: Subsystem = Subsystems.GooglePubSub
  }

  // Google Groups

  implicit object MemberMapper extends GoogleSubsystemMapper[Member] {
    override def subsystem: Subsystem = Subsystems.GoogleGroups
  }

  implicit object MembersMapper extends GoogleSubsystemMapper[Members] {
    override def subsystem: Subsystem = Subsystems.GoogleGroups
  }

  implicit object GroupMapper extends GoogleSubsystemMapper[Group] {
    override def subsystem: Subsystem = Subsystems.GoogleGroups
  }

  // Google Billing

  implicit object BillingAccountMapper extends GoogleSubsystemMapper[BillingAccount] {
    override def subsystem: Subsystem = Subsystems.GoogleBilling
  }

  implicit object ListBillingAccountsResponseMapper extends GoogleSubsystemMapper[ListBillingAccountsResponse] {
    override def subsystem: Subsystem = Subsystems.GoogleBilling
  }

  implicit object ProjectBillingInfoMapper extends GoogleSubsystemMapper[ProjectBillingInfo] {
    override def subsystem: Subsystem = Subsystems.GoogleBilling
  }

  // Google Genomics

  implicit object GenomicsOperationMapper extends GoogleSubsystemMapper[Operation] {
    override def subsystem: Subsystem = Subsystems.GoogleGenomics
  }

  implicit object ListOperationsResponseMapper extends GoogleSubsystemMapper[ListOperationsResponse] {
    override def subsystem: Subsystem = Subsystems.GoogleGenomics
  }
}
